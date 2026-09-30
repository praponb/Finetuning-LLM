WITH ranked_stores AS (SELECT id, region, opened_date, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS rn FROM stores) SELECT * FROM ranked_stores WHERE rn <= 5
