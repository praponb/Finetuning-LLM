SELECT
    p.paid_at,
    p.order_id,
    o.customer_id,
    o.order_date,
    o.status
FROM payments p
LEFT OUTER JOIN orders o ON p.order_id = o.id
WHERE o.channel IN ('partner', 'store', 'mobile')
ORDER BY p.paid_at;
