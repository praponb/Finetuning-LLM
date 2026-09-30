SELECT id, region, opened_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY opened_date DESC NULLS LAST, id) AS dedup_rank FROM stores) t WHERE dedup_rank = 1
