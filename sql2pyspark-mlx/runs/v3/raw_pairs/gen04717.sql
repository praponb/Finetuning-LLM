SELECT
    s.id,
    s.customer_id,
    s.plan,
    c.email
FROM subscriptions s
FULL OUTER JOIN customers c ON c.id = s.customer_id
WHERE s.customer_id IN (20, 37)
ORDER BY s.plan;
