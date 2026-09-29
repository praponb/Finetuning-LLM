SELECT order_date FROM orders WHERE order_date IS NOT NULL UNION SELECT sale_date FROM store_sales WHERE sale_date IS NOT NULL
