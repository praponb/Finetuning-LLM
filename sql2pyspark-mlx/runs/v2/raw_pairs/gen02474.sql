SELECT
    o.discount,
    o.shipping_fee,
    p.status,
    p.paid_at,
    CASE WHEN p.amount >= 1290 THEN 'High' WHEN p.amount >= 430 THEN 'Medium' ELSE 'Low' END AS amount_band
FROM orders o
LEFT JOIN payments p ON o.id = p.order_id
WHERE o.status IN ('CANCELLED', 'RETURNED')
