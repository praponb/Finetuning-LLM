SELECT store_id, SUM(quantity) total_quantity, SUM(SUM(quantity)) OVER (ORDER BY store_id) AS cumulative_quantity FROM inventory GROUP BY store_id ORDER BY total_quantity DESC
