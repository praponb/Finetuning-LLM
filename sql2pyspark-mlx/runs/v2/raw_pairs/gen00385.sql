SELECT
    o.id,
    o.amount,
    s.region,
    c.name,
    c.credit_limit
FROM orders o
JOIN stores s ON o.store_id = s.id
FULL OUTER JOIN customers c ON o.customer_id = c.id
ORDER BY name;
