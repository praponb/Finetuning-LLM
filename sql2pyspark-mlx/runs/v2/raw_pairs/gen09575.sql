SELECT * FROM (SELECT channel, status, shipping_fee FROM orders)
PIVOT (AVG(shipping_fee) FOR status IN ('PENDING', 'RETURNED', 'COMPLETED', 'SHIPPED'))
ORDER BY channel
