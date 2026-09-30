SELECT store_id, SUM(quantity) AS total_quantity, SUM(SUM(quantity)) OVER (ORDER BY store_id) AS cumulative_quantity FROM inventory WHERE MONTH(last_restock) IN (8, 9, 10) GROUP BY store_id;
