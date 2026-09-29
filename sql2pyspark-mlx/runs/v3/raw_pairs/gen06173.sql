SELECT country, 'customer' AS source FROM customers WHERE country = 'SG' UNION ALL SELECT country, 'session' AS source FROM sessions;
