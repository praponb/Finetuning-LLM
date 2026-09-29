SELECT city FROM customers UNION SELECT city FROM stores WHERE city NOT IN ('Bangkok', 'New York');
