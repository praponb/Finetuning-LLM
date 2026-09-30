SELECT
    o.id,
    o.order_date,
    o.shipping_fee,
    o.discount
FROM orders o
WHERE EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.amount BETWEEN 1180 AND 1370);
