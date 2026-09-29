SELECT country FROM suppliers UNION SELECT country FROM sessions WHERE customer_id IN (17, 25, 29, 36) ORDER BY country;
