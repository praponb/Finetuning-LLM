SELECT
    p.paid_at,
    p.amount,
    p.status,
    o.store_id,
    o.customer_id,
    o.order_date
FROM payments p
FULL OUTER JOIN orders o ON p.order_id = o.id
ORDER BY amount DESC
