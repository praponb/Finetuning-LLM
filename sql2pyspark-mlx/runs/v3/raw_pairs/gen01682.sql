SELECT * FROM (SELECT status, channel, shipping_fee FROM orders WHERE store_id = 9) PIVOT (SUM(shipping_fee) FOR channel IN ('mobile', 'partner', 'web')) ORDER BY status
