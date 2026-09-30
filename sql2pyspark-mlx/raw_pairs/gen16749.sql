SELECT signup_date FROM customers WHERE signup_date IS NOT NULL UNION SELECT order_date AS signup_date FROM orders WHERE order_date IS NOT NULL
