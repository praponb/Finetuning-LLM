SELECT s.status, c.email
FROM subscriptions s
INNER JOIN customers c ON c.id = s.customer_id
WHERE s.start_date >= '2024-04-01'
ORDER BY status;
