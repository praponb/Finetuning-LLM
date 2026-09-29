SELECT
    c.id,
    c.name,
    c.signup_date,
    c.city
FROM customers c
WHERE EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id)
ORDER BY id
