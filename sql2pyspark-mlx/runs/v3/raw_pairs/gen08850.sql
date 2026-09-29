SELECT
    p.id,
    p.order_id,
    o.store_id,
    o.shipping_fee,
    o.customer_id
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
WHERE p.amount <= 1220
