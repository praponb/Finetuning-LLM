SELECT city, country, 'customer' AS kind FROM customers UNION SELECT city, country, 'supplier' AS kind FROM suppliers;
