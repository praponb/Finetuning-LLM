SELECT o.order_date, c.signup_date, c.age
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
WHERE c.is_active = TRUE AND o.status IN ('RETURNED', 'PENDING')
ORDER BY o.order_date DESC;
