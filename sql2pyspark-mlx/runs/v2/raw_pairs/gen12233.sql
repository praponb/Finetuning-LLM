SELECT * FROM (SELECT product_id, COUNT(*) AS row_count, COUNT(units) AS units_count FROM store_sales GROUP BY product_id) AS t WHERE units_count >= 8;
