SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC NULLS FIRST, id ASC) AS dedup_rank FROM inventory) x WHERE dedup_rank = 1;
