SELECT * FROM (SELECT status, channel, shipping_fee FROM orders)
PIVOT (MAX(shipping_fee) FOR channel IN ('partner', 'web'));
