SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit DESC, id) AS dedup_rank FROM customers QUALIFY dedup_rank = 1
