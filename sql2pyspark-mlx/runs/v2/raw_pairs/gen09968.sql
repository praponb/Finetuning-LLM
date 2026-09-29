SELECT city FROM customers UNION SELECT city FROM stores WHERE MONTH(opened_date) IN (5, 8, 10)
