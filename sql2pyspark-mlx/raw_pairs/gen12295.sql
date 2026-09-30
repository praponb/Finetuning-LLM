SELECT
    s.id,
    s.customer_id,
    s.status,
    s.end_date
FROM subscriptions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE segment = 'Consumer');
