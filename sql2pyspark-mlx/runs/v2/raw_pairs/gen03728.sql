SELECT product_id, COUNT(DISTINCT units) AS unique_stores
FROM store_sales
WHERE revenue IS NULL
GROUP BY product_id
ORDER BY unique_stores DESC, product_id ASC
