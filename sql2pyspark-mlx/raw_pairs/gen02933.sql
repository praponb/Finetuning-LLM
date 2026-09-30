SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY cost DESC, id) AS dedup_rank FROM products) t WHERE dedup_rank = 1
