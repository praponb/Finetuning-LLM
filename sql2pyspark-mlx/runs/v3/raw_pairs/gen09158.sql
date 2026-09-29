SELECT DATE_FORMAT(orders.order_date, 'yyyy-MM') AS order_month, SUM(CASE WHEN shipments.carrier = 'UPS' THEN 1 ELSE 0 END) AS ups_count, COUNT(DISTINCT shipments.cost) AS unique_orders
FROM shipments
RIGHT JOIN orders ON orders.id = shipments.order_id
JOIN customers ON orders.customer_id = customers.id
GROUP BY DATE_FORMAT(orders.order_date, 'yyyy-MM')
ORDER BY order_month, ups_count DESC
