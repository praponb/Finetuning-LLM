SELECT
    s.plan,
    s.status,
    c.city,
    CAST(s.monthly_fee AS INT) AS monthly_fee_int
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id;
