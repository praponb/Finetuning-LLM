SELECT * FROM (SELECT DATE_FORMAT(order_date, 'yyyy-MM') AS order_month, channel, amount FROM orders)
PIVOT (MAX(amount) FOR channel IN ('mobile', 'store', 'partner', 'web'))
ORDER BY order_month;
