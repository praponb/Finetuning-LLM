SELECT *
FROM orders
WHERE channel IN ('store', 'mobile') AND status IN ('PENDING', 'CANCELLED', 'SHIPPED')
