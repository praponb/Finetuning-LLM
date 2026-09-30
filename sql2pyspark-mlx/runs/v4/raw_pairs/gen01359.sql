SELECT product_id, SUM(quantity) AS total_quantity, COUNT(*) AS cnt, SUM(SUM(quantity)) OVER (ORDER BY product_id) AS cumulative_quantity FROM inventory GROUP BY product_id
