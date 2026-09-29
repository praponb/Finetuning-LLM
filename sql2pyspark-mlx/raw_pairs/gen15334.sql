SELECT country, email, credit_limit, LOWER(country) AS country_lower FROM customers WHERE email LIKE '%@company.co' AND country = 'DE' ORDER BY email, credit_limit DESC;
