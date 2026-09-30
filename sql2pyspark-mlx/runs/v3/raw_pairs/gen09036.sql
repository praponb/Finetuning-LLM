SELECT p.paid_at, p.method, o.shipping_fee
FROM payments p
FULL OUTER JOIN orders o ON p.order_id = o.id
WHERE o.shipping_fee <= 32;
