SELECT
    s.id,
    s.monthly_fee,
    s.start_date,
    c.city
FROM subscriptions s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE c.city IN ('New York', 'Singapore');
