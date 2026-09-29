SELECT
    c.is_active,
    c.age,
    c.email,
    s.monthly_fee,
    CAST(s.monthly_fee AS INT) AS monthly_fee_int
FROM customers c
LEFT JOIN subscriptions s ON s.customer_id = c.id
WHERE s.customer_id = 11
ORDER BY c.age DESC;
