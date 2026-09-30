SELECT * FROM (SELECT product_id, store_id, COUNT(DISTINCT units) AS unique_stores FROM store_sales GROUP BY product_id, store_id) AS t WHERE unique_stores >= 4;
