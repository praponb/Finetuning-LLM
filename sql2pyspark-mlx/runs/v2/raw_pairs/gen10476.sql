SELECT
    c.is_active,
    c.age,
    c.email,
    s.browser,
    CASE WHEN c.city = 'Chiang Mai' THEN 1 ELSE 0 END AS is_chiang_mai
FROM customers c
LEFT JOIN sessions s ON s.customer_id = c.id
ORDER BY browser ASC, email DESC
