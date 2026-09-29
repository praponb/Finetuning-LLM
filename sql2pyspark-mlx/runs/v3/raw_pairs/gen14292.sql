SELECT
    o.id,
    o.shipping_fee,
    o.discount,
    o.order_date
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id)
ORDER BY o.id;
