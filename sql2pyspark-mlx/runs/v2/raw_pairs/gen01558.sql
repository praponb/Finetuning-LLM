SELECT status, DATE_TRUNC('MONTH', order_date) AS order_month, MAX(discount) AS max_discount FROM orders GROUP BY status, DATE_TRUNC('MONTH', order_date)
