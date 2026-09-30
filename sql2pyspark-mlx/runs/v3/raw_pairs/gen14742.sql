SELECT store_id, SUM(revenue) total_revenue, RANK() OVER (ORDER BY SUM(revenue) DESC) AS rnk FROM store_sales WHERE store_id IN (2, 3, 6) GROUP BY store_id
