SELECT status, discount, shipping_fee
FROM orders
WHERE status <> 'CANCELLED'
ORDER BY status DESC, discount DESC
