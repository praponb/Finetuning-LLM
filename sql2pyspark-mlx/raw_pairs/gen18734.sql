SELECT
    p.paid_at,
    p.status,
    o.customer_id,
    o.shipping_fee,
    o.order_date
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
WHERE o.customer_id IN (6, 10) AND p.method IN ('cash', 'card', 'promptpay');
