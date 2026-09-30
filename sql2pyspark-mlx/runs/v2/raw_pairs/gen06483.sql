SELECT orders.status, SUM(order_items.unit_price) AS total_unit_price, SUM(order_items.quantity) AS total_quantity
FROM order_items
RIGHT OUTER JOIN orders ON orders.id = order_items.order_id
RIGHT OUTER JOIN customers ON orders.customer_id = customers.id
WHERE order_items.quantity BETWEEN 3 AND 4
GROUP BY orders.status
