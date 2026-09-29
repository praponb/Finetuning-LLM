SELECT is_active, country, COUNT(DISTINCT credit_limit) AS unique_countrys, MIN(age) AS min_age FROM customers WHERE country != 'SG' AND is_active = TRUE GROUP BY is_active, country ORDER BY country
