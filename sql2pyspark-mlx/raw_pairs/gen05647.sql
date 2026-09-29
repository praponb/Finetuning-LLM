SELECT channel, MAX(order_date) AS last_order_date, ROUND(AVG(discount), 2) AS avg_discount, MIN(shipping_fee) AS min_shipping_fee FROM orders WHERE order_date >= '2024-04-01' GROUP BY 1
