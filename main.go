package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shogo82148/go-mecab"
)

var use_line_numbers = flag.Bool("line-id", false, "use line numbers instead of sentence IDs")
var input_file = flag.String("i", "jpn_sentences.tsv", "location of tatoeba sentence file")
var output_file = flag.String("o", "jpn_sentence_lookup.json", "location of output file")

type Sentence struct {
	id   int
	text []string
}

type Line struct {
	number  int
	content string
}

func scan_lines(lines chan<- Line) {
	file, err := os.Open(*input_file)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for line_number := 1; scanner.Scan(); line_number++ {
		line := scanner.Text()
		lines <- Line{line_number, line}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	close(lines)
}

func process_lines(wg *sync.WaitGroup, lines <-chan Line, sentences chan<- Sentence) {
	tagger, err := mecab.New(map[string]string{"output-format-type": "wakati"})
	defer func() {
		tagger.Destroy()
		wg.Done()
	}()

	if err != nil {
		panic(err)
	}

	for line := range lines {
		parts := strings.SplitN(line.content, "\t", 3)
		if len(parts) != 3 {
			panic(fmt.Sprintf("invalid data on line %d: %s", line.number, line.content))
		}

		if parts[1] != "jpn" {
			continue
		}

		var id int
		var err error

		if *use_line_numbers {
			id = line.number - 1
		} else if id, err = strconv.Atoi(parts[0]); err != nil {
			panic(fmt.Sprintf("invalid id on line %d: '%s'", line.number, parts[0]))
		}

		text := parts[2]
		words := make([]string, 0)

		for node, _ := tagger.ParseToNode(text); node.Next().Stat() != mecab.EOSNode; node = node.Next() {
			if node.Stat() == mecab.BOSNode || node.Stat() == mecab.EOSNode {
				continue
			}
			feature := node.Feature()
			reg_form := strings.Split(feature, ",")[6]
			if reg_form == "*" {
				reg_form = node.Surface()
			}
			words = append(words, reg_form)
		}

		sentences <- Sentence{id, words}
	}
}

func create_lookup_table(sentences <-chan Sentence, result chan<- map[string][]int) {
	lookup_table := make(map[string][]int)

	for sentence := range sentences {
		for _, word := range sentence.text {
			if _, ok := lookup_table[word]; !ok {
				lookup_table[word] = make([]int, 0)
			}
			lookup_table[word] = append(lookup_table[word], sentence.id)
		}
	}

	result <- lookup_table
}

func main() {
	flag.Parse()

	start := time.Now()

	var wg sync.WaitGroup
	lines := make(chan Line)
	sentences := make(chan Sentence)
	result := make(chan map[string][]int, 1)

	go scan_lines(lines)

	for i := 0; i < 20; i++ {
		go process_lines(&wg, lines, sentences)
		wg.Add(1)
	}

	go func() {
		wg.Wait()
		close(sentences)
	}()

	go create_lookup_table(sentences, result)

	lookup_table := <-result

	file, err := os.Create(*output_file)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	file_content, err := json.Marshal(lookup_table)

	if err != nil {
		panic(err)
	}

	if _, err := file.Write(file_content); err != nil {
		panic(err)
	}

	fmt.Printf("Done in %s\n", time.Since(start).Round(time.Millisecond))
	fmt.Printf("Keys: %d\n", len(lookup_table))
	fmt.Printf("Size: %d KB (%.2f MB)\n", len(file_content)/1024, float64(len(file_content))/1024/1024)
}
