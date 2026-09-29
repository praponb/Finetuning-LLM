SELECT
    s.id,
    s.customer_id,
    s.status,
    c.segment,
    c.email
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.credit_limit >= 11300 AND s.status IN ('CANCELLED', 'PAUSED')
