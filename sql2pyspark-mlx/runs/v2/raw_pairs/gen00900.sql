SELECT s.start_date, s.customer_id, c.is_active
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id;
