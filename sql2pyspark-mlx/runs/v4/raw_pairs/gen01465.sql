SELECT * FROM (SELECT status, channel, discount FROM orders) PIVOT (MAX(discount) FOR channel IN ('web', 'mobile', 'partner', 'store'))
