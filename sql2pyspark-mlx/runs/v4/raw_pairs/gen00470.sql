SELECT * FROM (SELECT status, store_id, channel, shipping_fee FROM orders) src PIVOT (AVG(shipping_fee) FOR channel IN ('mobile' AS mobile, 'partner' AS partner));
