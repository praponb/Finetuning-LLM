SELECT
    p.method,
    p.paid_at,
    o.customer_id,
    o.shipping_fee,
    s.region
FROM payments p
FULL OUTER JOIN orders o ON p.order_id = o.id
LEFT JOIN stores s ON s.id = o.store_id
WHERE p.status <> 'REFUNDED'
