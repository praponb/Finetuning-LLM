SELECT store_id, MAX(units) - MIN(units) AS units_range, COUNT(*) AS cnt FROM store_sales GROUP BY store_id
