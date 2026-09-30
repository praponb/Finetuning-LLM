SELECT city, 'customer' AS source FROM customers UNION ALL SELECT city, 'store' AS source FROM stores WHERE city IN ('London', 'Tokyo', 'Chiang Mai') ORDER BY city DESC;
