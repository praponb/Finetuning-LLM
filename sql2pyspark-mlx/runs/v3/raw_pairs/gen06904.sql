SELECT * FROM (SELECT customer_id, channel, shipping_fee FROM orders) PIVOT (SUM(shipping_fee) FOR channel IN ('web', 'mobile', 'store')) ORDER BY customer_id
