SELECT * FROM (SELECT QUARTER(order_date) AS order_quarter, channel, shipping_fee FROM orders) PIVOT (COUNT(shipping_fee) FOR channel IN ('partner', 'mobile')) ORDER BY order_quarter
