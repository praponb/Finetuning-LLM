SELECT
    s.carrier,
    s.shipped_date,
    o.status,
    o.store_id,
    c.segment,
    c.age
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
LEFT OUTER JOIN customers c ON o.customer_id = c.id
WHERE o.status IN ('COMPLETED', 'CANCELLED', 'PENDING')
ORDER BY 5 DESC
