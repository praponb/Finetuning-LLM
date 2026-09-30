SELECT
    o.id,
    o.shipping_fee,
    o.channel,
    o.customer_id
FROM orders o
WHERE o.discount BETWEEN 0.29 AND 0.33 AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id);
