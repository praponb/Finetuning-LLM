SELECT s.monthly_fee, c.age
FROM subscriptions s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE s.customer_id IS NOT NULL;
