SELECT
    o.id,
    o.amount,
    o.store_id,
    s.city,
    s.store_name,
    c.is_active,
    c.age
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
FULL OUTER JOIN customers c ON o.customer_id = c.id;
