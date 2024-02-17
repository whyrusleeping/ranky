package main

import (
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"sort"
	"time"

	"encoding/csv"

	"github.com/grd/histogram"
)

func main() {
	fmt.Println("vim-go")

	if err := Main(); err != nil {
		panic(err)
	}
}

func Main() error {
	pfi, err := os.Create("cpu.prof")
	if err != nil {
		return err
	}

	pprof.StartCPUProfile(pfi)
	defer pprof.StopCPUProfile()

	pr := NewGraph()

	fi, err := os.Open("../seeemore/graph_snapshot.csv")
	if err != nil {
		return err
	}
	defer fi.Close()

	r := csv.NewReader(fi)

	_, err = r.Read()
	if err != nil {
		return err
	}

	umap := make(map[string]int)
	imap := make(map[int]string)

	id := int(0)
	idForDid := func(s string) int {
		v, ok := umap[s]
		if !ok {
			v = id
			umap[s] = id
			imap[id] = s
			id++
		}

		return v
	}

	for {
		row, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		from := row[0]
		to := row[1]
		follow := row[7] == "t"
		block := row[6] == "t"

		if !block && !follow {
			continue
		}

		fid := idForDid(from)
		tid := idForDid(to)

		if block {
			pr.AddEdge(fid, tid, -1)
		} else if follow {
			pr.AddEdge(fid, tid, 1)
		}
	}

	good := []string{
		"did:plc:vpkhqolt662uhesyj6nxm7ys", // why
		"did:plc:ragtjsm2j2vknwkz3zp4oxrd", // paul
		"did:plc:oky5czdrnfjpqslsw2a5iclo", // jay
	}

	for _, d := range good {
		pr.GoodList = append(pr.GoodList, idForDid(d))
	}

	fmt.Println("Graph complete, ranking now...", time.Now())
	ranks := pr.PageRank(20, 0.85)
	fmt.Println("ranking finished: ", time.Now())
	/*
		pr.Rank(0.85, 0.0001, func(id int, rank float64) {
			if checkids[id] {
				fmt.Println(imap[id], rank)
			}
		})
	*/

	out := make([]rankpair, 0, len(ranks))
	for id, v := range ranks {
		out = append(out, rankpair{id: id, val: v})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].val < out[j].val
	})

	fmt.Println("lowest rank: ")
	for i := 0; i < 10; i++ {
		fmt.Println(imap[out[i].id], out[i].val)
	}

	fmt.Println("highest rank: :")
	for i := len(out) - 15; i < len(out); i++ {
		fmt.Println(imap[out[i].id], out[i].val)
	}

	hist, _ := histogram.NewHistogram([]float64{-1, 0, 0.0000001, 0.0000002, 0.0000004, 0.0000006, 0.0000008, 0.000001, 0.000002, 0.000005, 0.00001, 0.0001, 0.1})
	for i := 0; i < len(out); i++ {
		hist.Add(out[i].val)
	}
	fmt.Println(hist.String())
	fmt.Println("median rank: ", out[len(out)/2].val)

	return nil
}

type rankpair struct {
	id  int
	val float64
}

type Node struct {
	ID      int
	Weight  float64
	Outlink []link
}

type link struct {
	to  int
	val float64
}

type Graph struct {
	//Nodes map[int]*Node
	Nodes []*Node

	GoodList []int
	BadList  []int
}

func NewGraph() *Graph {
	return &Graph{
		Nodes: make([]*Node, 0, 2000000),
	}
}

func (g *Graph) AddNode(id int) {
	if int(id) >= len(g.Nodes) {
		g.Nodes = append(g.Nodes, make([]*Node, int(id)-len(g.Nodes)+1)...)
	}

	g.Nodes[id] = &Node{
		ID: id,
	}
}

func (g *Graph) getNode(v int) *Node {
	if int(v) >= len(g.Nodes) {
		return nil
	}

	return g.Nodes[v]
}

func (g *Graph) AddEdge(src, dest int, weight float64) {
	if n := g.getNode(src); n == nil {
		g.AddNode(src)
	}
	if n := g.getNode(dest); n == nil {
		g.AddNode(dest)
	}

	g.Nodes[src].Outlink = append(g.Nodes[src].Outlink, link{to: dest, val: weight})
}

func (g *Graph) PageRank(iterations int, d float64) []float64 {
	rank := make([]float64, len(g.Nodes))
	newRank := make([]float64, len(g.Nodes))

	n := float64(len(g.Nodes))
	for id := range g.Nodes {
		rank[id] = 1.0 / n
	}

	for _, n := range g.GoodList {
		rank[n] = 1
	}

	for _, n := range g.BadList {
		rank[n] = -1
	}

	for i := 0; i < iterations; i++ {
		for _, node := range g.Nodes {
			if node == nil {
				continue
			}

			newRank[node.ID] = (1 - d) / n
		}

		for _, node := range g.Nodes {
			if node == nil {
				continue
			}

			currentRank := rank[node.ID]

			totalPositiveWeight := 0.0
			totalNegativeWeight := 0.0
			for _, lnk := range node.Outlink {
				if lnk.val > 0 {
					totalPositiveWeight += lnk.val
				} else {
					totalNegativeWeight -= lnk.val
				}
			}

			for _, lnk := range node.Outlink {
				if lnk.val > 0 {
					newRank[lnk.to] += d * currentRank * lnk.val / totalPositiveWeight
				} else {
					newRank[lnk.to] += d * currentRank * lnk.val / totalNegativeWeight
				}
			}
		}

		for id := range rank {
			rank[id] = newRank[id]
			newRank[id] = 0 // reset for next iteration
		}
	}

	return rank
}
