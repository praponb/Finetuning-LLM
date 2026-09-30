SELECT city, 'customer' AS source FROM customers WHERE segment = 'SMB' UNION SELECT city, 'store' AS source FROM stores WHERE city = 'Bangkok';
