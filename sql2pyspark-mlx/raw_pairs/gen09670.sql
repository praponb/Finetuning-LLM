SELECT id, name, email, credit_limit, age FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY age DESC NULLS LAST, id) AS dedup_rank FROM customers) t WHERE dedup_rank = 1
