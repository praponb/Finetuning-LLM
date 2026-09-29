SELECT signup_date FROM customers WHERE credit_limit IS NOT NULL UNION SELECT delivered_date FROM shipments ORDER BY signup_date
