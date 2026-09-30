SELECT s.monthly_fee, c.email, c.age
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
