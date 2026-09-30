SELECT DATE_FORMAT(order_date, 'yyyy-MM') AS order_month, COUNT(discount) AS discount_count
FROM orders
WHERE shipping_fee BETWEEN 19 AND 25
GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
