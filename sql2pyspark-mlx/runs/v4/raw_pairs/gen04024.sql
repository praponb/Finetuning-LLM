SELECT order_id, COUNT(DISTINCT unit_price) AS unique_orders, AVG(quantity) AS avg_quantity
FROM order_items
GROUP BY order_id
HAVING COUNT(DISTINCT unit_price) > 1;
