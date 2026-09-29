SELECT DATE_FORMAT(o.order_date, 'yyyy-MM') AS order_month, ROUND(AVG(oi.quantity), 2) AS avg_quantity, COUNT(DISTINCT oi.unit_price) AS unique_orders
FROM order_items oi
INNER JOIN orders o ON o.id = oi.order_id
RIGHT OUTER JOIN customers c ON o.customer_id = c.id
WHERE oi.unit_price <= 250
GROUP BY DATE_FORMAT(o.order_date, 'yyyy-MM')
ORDER BY avg_quantity DESC, unique_orders DESC;
