SELECT * FROM (SELECT DATE_FORMAT(order_date, 'yyyy-MM') AS order_month, status, shipping_fee FROM orders)
PIVOT (COUNT(shipping_fee) FOR status IN ('COMPLETED', 'CANCELLED'))
ORDER BY order_month
