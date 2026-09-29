SELECT city, 'customer' source FROM customers UNION ALL SELECT city, 'store' source FROM stores ORDER BY source DESC;
