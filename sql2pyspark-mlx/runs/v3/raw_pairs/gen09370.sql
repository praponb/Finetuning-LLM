SELECT
    s.plan,
    s.monthly_fee,
    c.age,
    CONCAT(s.status, '/', s.plan) AS status_plan
FROM subscriptions s
INNER JOIN customers c ON c.id = s.customer_id
WHERE c.city IN ('New York', 'Berlin')
ORDER BY c.age DESC;
