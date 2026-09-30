SELECT
    s.id,
    s.delivered_date,
    s.shipped_date,
    o.shipping_fee,
    o.status
FROM shipments s
LEFT JOIN orders o ON o.id = s.order_id
ORDER BY status ASC, id DESC
LIMIT 1;
