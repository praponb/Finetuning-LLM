WITH ranked_stores AS (SELECT id, region, opened_date, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) rnk FROM stores) SELECT * FROM ranked_stores WHERE rnk = 1
