SELECT * FROM (SELECT status, channel, amount FROM orders) PIVOT (SUM(amount) FOR channel IN ('web', 'mobile'))
