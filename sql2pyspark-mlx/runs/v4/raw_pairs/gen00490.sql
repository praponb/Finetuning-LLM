SELECT city, 'customer' AS source FROM customers WHERE country = 'TH' UNION SELECT city, 'store' AS source FROM stores WHERE city != 'Chiang Mai' ORDER BY city;
