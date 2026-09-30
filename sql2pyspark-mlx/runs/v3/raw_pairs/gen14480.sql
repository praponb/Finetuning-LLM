SELECT country, 'customer' AS source FROM customers WHERE name LIKE 'Ja%' UNION ALL SELECT country, 'session' AS source FROM sessions ORDER BY source DESC;
