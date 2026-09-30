SELECT DATE_FORMAT(order_date, 'yyyy-MM') AS order_month, MAX(shipping_fee) - MIN(shipping_fee) AS shipping_fee_range, MIN(discount) AS min_discount
FROM orders
GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
HAVING MAX(shipping_fee) - MIN(shipping_fee) >= 76;
