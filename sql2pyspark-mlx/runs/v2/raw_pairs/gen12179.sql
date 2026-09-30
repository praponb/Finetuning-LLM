SELECT
    s.end_date,
    s.customer_id,
    c.city,
    c.name,
    c.age
FROM subscriptions s
FULL OUTER JOIN customers c ON c.id = s.customer_id
WHERE c.credit_limit >= 15500
ORDER BY c.city;
