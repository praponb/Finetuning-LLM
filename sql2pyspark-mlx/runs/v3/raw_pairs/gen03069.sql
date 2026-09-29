SELECT * FROM (SELECT status, channel, amount FROM orders)
PIVOT (MAX(amount) FOR channel IN ('partner', 'web', 'mobile'))
