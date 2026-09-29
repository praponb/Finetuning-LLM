SELECT
    o.id,
    o.status,
    o.store_id,
    c.name,
    s.region
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
LEFT JOIN stores s ON o.store_id = s.id
WHERE LENGTH(c.name) > 7 OR c.signup_date <= '2024-03-01';
