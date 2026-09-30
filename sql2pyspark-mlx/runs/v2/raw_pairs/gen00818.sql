SELECT
    s.id,
    s.customer_id,
    s.device,
    c.city,
    c.name,
    c.email
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE c.age = 47
ORDER BY email;
