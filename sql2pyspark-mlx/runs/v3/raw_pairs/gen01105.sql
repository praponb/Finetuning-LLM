SELECT
    o.store_id,
    o.amount,
    c.country,
    s.sqft,
    LOWER(s.store_name) store_name_lower
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
RIGHT OUTER JOIN stores s ON o.store_id = s.id
WHERE o.status IN ('COMPLETED', 'RETURNED');
