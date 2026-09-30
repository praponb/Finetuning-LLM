SELECT country, city FROM customers WHERE country != 'DE' UNION SELECT country, city FROM suppliers ORDER BY city;
