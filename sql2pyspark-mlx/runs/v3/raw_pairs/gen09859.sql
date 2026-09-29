SELECT
    s.id,
    s.monthly_fee,
    s.customer_id,
    s.end_date
FROM subscriptions s
WHERE s.end_date <= '2024-10-01' AND s.customer_id IN (SELECT id FROM customers WHERE age = 66)
ORDER BY monthly_fee, customer_id DESC;
