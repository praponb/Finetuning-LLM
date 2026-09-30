SELECT product_id, store_id, COUNT(*) AS row_count, MAX(units) - MIN(units) AS units_range FROM store_sales GROUP BY product_id, store_id
