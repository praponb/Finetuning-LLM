SELECT city, country, 'customer' AS source FROM customers UNION ALL SELECT city, country, 'supplier' AS source FROM suppliers ORDER BY city;
