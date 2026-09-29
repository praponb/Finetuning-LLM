SELECT * FROM (SELECT channel, status, shipping_fee FROM orders)
PIVOT (SUM(shipping_fee) FOR status IN ('RETURNED', 'PENDING', 'SHIPPED'))
ORDER BY channel
