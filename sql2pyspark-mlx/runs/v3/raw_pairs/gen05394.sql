SELECT
    o.id,
    o.store_id,
    o.customer_id,
    o.status,
    c.segment,
    c.age,
    c.name,
    IF(c.is_active, 'Yes', 'No') is_active_label
FROM orders o
FULL OUTER JOIN customers c ON o.customer_id = c.id
