SELECT * FROM (SELECT status, channel, discount FROM orders)
PIVOT (COUNT(discount) FOR channel IN ('mobile', 'store'))
ORDER BY status
