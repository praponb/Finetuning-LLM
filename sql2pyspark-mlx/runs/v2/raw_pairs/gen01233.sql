SELECT * FROM (SELECT channel, status, shipping_fee FROM orders WHERE discount < 0.33) PIVOT (AVG(shipping_fee) FOR status IN ('CANCELLED', 'PENDING')) ORDER BY channel;
