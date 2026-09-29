SELECT
    p.status,
    o.order_date,
    o.customer_id,
    o.shipping_fee
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
WHERE p.order_id = 10;
