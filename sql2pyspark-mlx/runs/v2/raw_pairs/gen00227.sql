SELECT product_id, COUNT(DISTINCT revenue) AS unique_stores
FROM store_sales
WHERE sale_date BETWEEN '2023-06-26' AND '2025-09-01'
GROUP BY product_id
HAVING unique_stores < 2;
