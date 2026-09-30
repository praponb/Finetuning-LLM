SELECT 'customer' AS source, country FROM customers WHERE name IS NULL UNION ALL SELECT 'session' AS source, country FROM sessions;
