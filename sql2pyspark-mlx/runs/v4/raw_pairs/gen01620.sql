SELECT
    p.method,
    p.amount,
    p.order_id,
    o.shipping_fee,
    CASE WHEN o.amount >= 1390 THEN 'High' WHEN o.amount >= 270 THEN 'Medium' ELSE 'Low' END amount_band
FROM payments p
JOIN orders o ON p.order_id = o.id
