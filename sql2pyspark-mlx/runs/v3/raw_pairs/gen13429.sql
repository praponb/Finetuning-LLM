SELECT QUARTER(c.signup_date) signup_quarter, SUM(oi.quantity * oi.unit_price) revenue, COUNT(*) num_order_items
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
JOIN customers c ON o.customer_id = c.id
WHERE c.name LIKE 'H%'
GROUP BY QUARTER(c.signup_date);
