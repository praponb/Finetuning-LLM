SELECT id, shipping_fee, NTILE(3) OVER (ORDER BY shipping_fee, id) AS shipping_fee_ntile
FROM orders
WHERE status NOT IN ('PENDING', 'COMPLETED', 'SHIPPED')
