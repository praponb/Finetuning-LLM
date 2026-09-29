SELECT MONTH(order_date) AS order_month, ROUND(STDDEV(discount), 2) AS stddev_discount, AVG(shipping_fee) AS avg_shipping_fee FROM orders GROUP BY MONTH(order_date) ORDER BY stddev_discount DESC;
