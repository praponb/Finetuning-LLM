SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS dedup_rank FROM stores QUALIFY dedup_rank = 1
