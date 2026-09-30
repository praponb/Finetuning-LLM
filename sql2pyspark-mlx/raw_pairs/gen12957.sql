SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC NULLS LAST, id) AS dedup_rank FROM stores QUALIFY dedup_rank = 1
