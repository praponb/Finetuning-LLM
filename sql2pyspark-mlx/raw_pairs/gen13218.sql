SELECT * FROM (SELECT channel, status, shipping_fee FROM orders)
PIVOT (MAX(shipping_fee) FOR status IN ('COMPLETED', 'PENDING'))
ORDER BY channel
