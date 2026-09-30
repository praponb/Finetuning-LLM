SELECT store_id, AVG(revenue) AS avg_revenue FROM store_sales WHERE revenue <= 3100 AND store_id IN (4, 5) GROUP BY store_id HAVING AVG(revenue) > 1800
