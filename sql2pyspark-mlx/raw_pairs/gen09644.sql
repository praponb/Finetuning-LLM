SELECT
    c.id,
    c.age,
    c.signup_date,
    c.credit_limit
FROM customers c
WHERE c.city = 'Chiang Mai' AND c.id IN (SELECT customer_id FROM subscriptions WHERE customer_id IN (6, 25))
ORDER BY age DESC;
