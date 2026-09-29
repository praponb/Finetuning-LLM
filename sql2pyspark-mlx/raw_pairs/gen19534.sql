SELECT
    s.id,
    s.cost,
    s.order_id,
    s.delivered_date
FROM shipments s
WHERE s.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee >= 29)
