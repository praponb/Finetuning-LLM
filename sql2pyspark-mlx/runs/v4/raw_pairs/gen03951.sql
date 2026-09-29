SELECT order_id, SUM(quantity) AS total_quantity, SUM(SUM(quantity)) OVER (ORDER BY order_id) AS cumulative_quantity
FROM order_items
GROUP BY order_id
ORDER BY cumulative_quantity DESC
