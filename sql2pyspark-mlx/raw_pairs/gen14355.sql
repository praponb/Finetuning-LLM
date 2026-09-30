SELECT DATE_TRUNC('MONTH', order_date) AS order_month, MIN(order_date) AS first_order_date
FROM orders
WHERE customer_id IS NOT NULL OR customer_id = 14
GROUP BY DATE_TRUNC('MONTH', order_date)
