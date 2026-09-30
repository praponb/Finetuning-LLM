SELECT store_id, SUM(revenue) AS total_revenue, SUM(SUM(revenue)) OVER (ORDER BY store_id) AS cumulative_revenue
FROM store_sales
GROUP BY store_id
ORDER BY cumulative_revenue, store_id DESC
LIMIT 3;
