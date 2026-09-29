SELECT
    s.delivered_date,
    s.status,
    s.shipped_date,
    o.amount
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
WHERE o.amount > 1160 AND o.amount > 1070
ORDER BY shipped_date
