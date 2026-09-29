SELECT store_id, COUNT(DISTINCT revenue) AS unique_stores, COUNT(*) AS store_sale_count
FROM store_sales
WHERE product_id = 4 AND sale_date BETWEEN '2024-06-16' AND '2025-04-30'
GROUP BY store_id
HAVING store_sale_count < 3;
