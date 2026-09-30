SELECT country, 'session' AS record_type FROM sessions WHERE country IS NOT NULL UNION ALL SELECT name, 'customer' AS record_type FROM customers WHERE name IS NOT NULL
