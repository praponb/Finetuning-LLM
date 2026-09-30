SELECT
    s.shipped_date,
    s.status,
    s.order_id,
    o.status AS order_status,
    o.order_date,
    o.channel,
    CEIL(o.shipping_fee) AS shipping_fee_whole
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
WHERE s.status IN ('LOST', 'RETURNED')
ORDER BY s.order_id DESC, status;
