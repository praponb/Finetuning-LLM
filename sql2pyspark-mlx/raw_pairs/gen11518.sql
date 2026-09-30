SELECT DATE_TRUNC('MONTH', orders.order_date) AS order_month, MAX(shipments.cost) AS max_cost
FROM shipments
JOIN orders ON shipments.order_id = orders.id
JOIN customers ON customers.id = orders.customer_id
GROUP BY DATE_TRUNC('MONTH', orders.order_date)
HAVING MAX(shipments.cost) > 89
ORDER BY max_cost DESC;
