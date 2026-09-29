SELECT country, 'customer' AS source FROM customers WHERE age < 31 UNION SELECT country, 'session' AS source FROM sessions WHERE customer_id = 15 ORDER BY source;
