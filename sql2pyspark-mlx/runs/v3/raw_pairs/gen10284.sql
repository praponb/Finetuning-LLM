SELECT * FROM (SELECT status, channel, discount FROM orders)
PIVOT (SUM(discount) FOR channel IN ('web', 'partner', 'mobile', 'store'));
