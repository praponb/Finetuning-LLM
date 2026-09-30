SELECT store_id, AVG(revenue) AS avg_revenue
FROM store_sales
GROUP BY store_id
ORDER BY store_id DESC
LIMIT 50;
