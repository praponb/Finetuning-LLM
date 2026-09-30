SELECT
    p.method,
    o.shipping_fee,
    o.status,
    o.amount - o.discount AS amount_minus_discount
FROM payments p
JOIN orders o ON o.id = p.order_id
WHERE NOT (p.status IN ('FAILED', 'REFUNDED', 'SUCCESS') OR p.method <> 'bank_transfer')
