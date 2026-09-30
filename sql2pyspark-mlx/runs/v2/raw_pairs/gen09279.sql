SELECT channel, customer_id, MAX(shipping_fee) AS max_shipping_fee
FROM orders
WHERE status IN ('PENDING', 'CANCELLED', 'SHIPPED')
GROUP BY channel, customer_id
