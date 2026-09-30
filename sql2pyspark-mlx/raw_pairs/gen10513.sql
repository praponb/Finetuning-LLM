SELECT
    c.age,
    c.segment,
    s.customer_id,
    s.status
FROM customers c
LEFT JOIN subscriptions s ON s.customer_id = c.id
WHERE LENGTH(c.name) < 10
ORDER BY c.age;
