SELECT city, 'customer' AS source FROM customers WHERE city = 'Tokyo' UNION SELECT city, 'store' AS source FROM stores ORDER BY source;
