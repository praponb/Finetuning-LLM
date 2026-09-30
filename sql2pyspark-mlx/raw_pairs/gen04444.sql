SELECT 'customer' AS kind, country FROM customers WHERE city <> 'Singapore' UNION ALL SELECT 'session' AS kind, country FROM sessions WHERE country = 'DE' ORDER BY country DESC;
