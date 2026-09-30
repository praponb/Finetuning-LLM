SELECT store_id, MAX(revenue) AS max_revenue, ROUND(AVG(revenue), 2) AS avg_revenue FROM store_sales WHERE units <= 19 AND units IS NOT NULL AND units BETWEEN 15 AND 26 GROUP BY store_id
