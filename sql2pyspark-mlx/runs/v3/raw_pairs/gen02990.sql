SELECT
    o.id,
    o.customer_id,
    o.store_id,
    s.store_name,
    s.sqft,
    c.email,
    c.credit_limit
FROM orders o
FULL OUTER JOIN stores s ON o.store_id = s.id
INNER JOIN customers c ON o.customer_id = c.id
WHERE o.channel = 'store';
