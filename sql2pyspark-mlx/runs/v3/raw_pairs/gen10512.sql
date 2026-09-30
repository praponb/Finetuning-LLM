SELECT product_id, SUM(units) AS total_units, COUNT(DISTINCT revenue) AS unique_products
FROM store_sales
WHERE store_id IN (1, 2, 4, 6) AND store_id IN (3, 4, 5, 6)
GROUP BY product_id;
