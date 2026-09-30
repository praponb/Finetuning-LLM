SELECT store_id, COUNT(revenue) revenue_count
FROM store_sales
GROUP BY store_id
HAVING revenue_count >= 5;
