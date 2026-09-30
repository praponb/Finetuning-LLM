SELECT store_id, SUM(quantity) AS total_quantity, COUNT(*) AS cnt, SUM(SUM(quantity)) OVER (ORDER BY store_id) AS cumulative_quantity FROM inventory WHERE product_id = 2 GROUP BY store_id
