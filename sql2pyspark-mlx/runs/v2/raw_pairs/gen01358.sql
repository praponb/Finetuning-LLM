SELECT
    o.id,
    o.amount,
    c.city,
    s.sqft
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
LEFT JOIN stores s ON o.store_id = s.id
WHERE UPPER(s.store_name) LIKE '%IAM%'
