SELECT country, 'customer' source FROM customers UNION ALL SELECT country, 'session' source FROM sessions ORDER BY country;
