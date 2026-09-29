SELECT
    o.store_id,
    c.segment,
    c.email,
    s.sqft
FROM orders o
RIGHT JOIN customers c ON c.id = o.customer_id
LEFT JOIN stores s ON o.store_id = s.id
WHERE o.customer_id IS NOT NULL
ORDER BY c.email
