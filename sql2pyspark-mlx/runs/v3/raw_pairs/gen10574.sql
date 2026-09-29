SELECT signup_date, 'customer' source FROM customers WHERE is_active UNION ALL SELECT order_date, 'order' source FROM orders ORDER BY signup_date DESC
