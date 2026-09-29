SELECT
    c.id,
    c.credit_limit,
    c.age,
    c.name
FROM customers c
WHERE c.id IN (SELECT customer_id FROM subscriptions WHERE customer_id IN (6, 8, 18, 21))
