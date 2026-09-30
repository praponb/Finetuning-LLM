SELECT p.order_id, p.status, o.shipping_fee
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
ORDER BY shipping_fee DESC
