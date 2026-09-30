SELECT
    s.id,
    s.delivered_date,
    s.cost,
    o.order_date,
    o.shipping_fee,
    o.amount * o.discount AS amount_times_discount
FROM shipments s
FULL OUTER JOIN orders o ON s.order_id = o.id
WHERE s.order_id IN (13, 37);
