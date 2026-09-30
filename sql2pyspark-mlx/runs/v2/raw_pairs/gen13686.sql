SELECT
    o.id,
    o.discount,
    c.is_active,
    c.age,
    s.sqft
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
LEFT JOIN stores s ON o.store_id = s.id
WHERE s.opened_date < '2025-07-01' AND c.is_active
