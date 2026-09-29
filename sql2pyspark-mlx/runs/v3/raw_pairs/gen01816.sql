SELECT MONTH(order_date) AS order_month, SUM(shipping_fee) AS total_shipping_fee, SUM(SUM(shipping_fee)) OVER (ORDER BY MONTH(order_date)) AS cumulative_shipping_fee
FROM orders
GROUP BY MONTH(order_date)
ORDER BY order_month DESC, 2 DESC;
