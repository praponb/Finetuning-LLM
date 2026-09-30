SELECT
    s.id,
    s.plan,
    s.status,
    s.start_date
FROM subscriptions s
WHERE s.plan = 'standard' AND s.customer_id IN (SELECT id FROM customers WHERE age <= 39)
ORDER BY plan DESC;
