SELECT * FROM (SELECT status, channel, amount FROM orders)
PIVOT (AVG(amount) FOR channel IN ('partner', 'web', 'mobile', 'store'))
ORDER BY status
