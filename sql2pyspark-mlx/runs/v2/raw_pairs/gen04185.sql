SELECT MONTH(orders.order_date) AS order_month, orders.channel, SUM(order_items.unit_price * order_items.quantity) AS revenue
FROM order_items
RIGHT JOIN orders ON orders.id = order_items.order_id
JOIN customers ON customers.id = orders.customer_id
WHERE order_items.quantity IS NOT NULL
GROUP BY MONTH(orders.order_date), orders.channel
ORDER BY order_month DESC, channel ASC
LIMIT 10;
