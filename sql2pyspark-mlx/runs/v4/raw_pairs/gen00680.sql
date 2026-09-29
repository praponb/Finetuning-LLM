SELECT * FROM (SELECT customer_id, channel, amount FROM orders) src PIVOT (MAX(amount) FOR channel IN ('web', 'partner', 'mobile'));
