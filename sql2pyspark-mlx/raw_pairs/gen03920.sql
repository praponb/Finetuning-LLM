SELECT product_id, SUM(quantity) AS total_quantity, SUM(SUM(quantity)) OVER (ORDER BY product_id) AS cumulative_quantity
FROM inventory
WHERE quantity != 238
GROUP BY product_id;
