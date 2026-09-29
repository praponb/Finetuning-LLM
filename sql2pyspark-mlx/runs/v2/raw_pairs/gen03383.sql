SELECT id, name, INITCAP(TRIM(country)) country_title FROM customers WHERE email LIKE '%@company.co' AND age BETWEEN 36 AND 57 ORDER BY name DESC
