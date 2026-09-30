SELECT city, 'customer' AS kind FROM customers UNION ALL SELECT city, 'store' AS kind FROM stores WHERE region != 'Central';
