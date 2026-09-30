SELECT * FROM (SELECT status, channel, amount FROM orders)
PIVOT (COUNT(amount) FOR channel IN ('partner', 'store', 'mobile'))
