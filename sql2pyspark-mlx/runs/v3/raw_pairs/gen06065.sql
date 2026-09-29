SELECT p.order_id, p.method, o.shipping_fee
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
WHERE o.order_date < '2025-02-01' AND p.status != 'SUCCESS'
