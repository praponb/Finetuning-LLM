SELECT
    s.id,
    s.carrier,
    s.cost,
    o.store_id,
    o.order_date,
    c.country,
    c.email,
    TRIM(s.carrier) clean_carrier
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
FULL OUTER JOIN customers c ON c.id = o.customer_id
WHERE LENGTH(c.name) < 12
ORDER BY carrier DESC
