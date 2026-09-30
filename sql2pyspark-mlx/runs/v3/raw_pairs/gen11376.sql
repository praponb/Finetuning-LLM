SELECT MONTH(order_date) AS order_month, MAX(shipping_fee) - MIN(shipping_fee) AS shipping_fee_range
FROM orders
GROUP BY order_month;
