SELECT
    o.discount,
    s.city,
    s.region,
    c.credit_limit,
    c.email
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
LEFT JOIN customers c ON o.customer_id = c.id
ORDER BY c.credit_limit
