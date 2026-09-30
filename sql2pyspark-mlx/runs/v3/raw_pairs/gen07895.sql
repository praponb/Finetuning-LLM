SELECT order_id, SUM(quantity) AS total_quantity, MIN(quantity) AS min_quantity
FROM order_items
WHERE (unit_price BETWEEN 250 AND 260 AND order_id = 31) OR order_id = 19
GROUP BY order_id
ORDER BY min_quantity DESC, total_quantity DESC
