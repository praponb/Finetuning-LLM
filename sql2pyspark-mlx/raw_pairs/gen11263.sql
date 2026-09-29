SELECT
    s.id,
    s.shipped_date,
    s.status,
    o.status AS order_status,
    o.discount
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
ORDER BY o.discount DESC, id
