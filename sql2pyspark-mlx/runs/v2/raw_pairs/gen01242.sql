SELECT DATE_FORMAT(order_date, 'yyyy-MM') AS order_month, ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee
FROM orders
GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
HAVING ROUND(AVG(shipping_fee), 2) >= 44
ORDER BY order_month ASC
LIMIT 5;
