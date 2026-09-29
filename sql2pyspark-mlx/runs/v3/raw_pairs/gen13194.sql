SELECT
    p.id,
    p.amount,
    p.paid_at,
    o.order_date
FROM payments p
LEFT OUTER JOIN orders o ON p.order_id = o.id
WHERE p.method = 'card'
