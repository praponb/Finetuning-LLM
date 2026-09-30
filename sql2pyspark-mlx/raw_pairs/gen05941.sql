SELECT order_id, COUNT(DISTINCT quantity) AS unique_orders, ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM order_items
WHERE quantity <= 8 OR order_id IN (2, 8)
GROUP BY order_id
