SELECT product_id, MAX(revenue) AS max_revenue, COUNT(*) AS total_count
FROM store_sales
GROUP BY product_id
HAVING COUNT(*) >= 3;
