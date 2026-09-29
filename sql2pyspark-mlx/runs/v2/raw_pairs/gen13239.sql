SELECT order_id, COUNT(DISTINCT quantity) AS unique_orders
FROM order_items
WHERE order_id IN (11, 35, 39, 40)
GROUP BY 1
ORDER BY unique_orders;
