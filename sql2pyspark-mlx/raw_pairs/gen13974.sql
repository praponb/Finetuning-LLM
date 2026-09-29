SELECT country, 'supplier' AS record_type FROM suppliers UNION SELECT country, 'session' AS record_type FROM sessions WHERE browser IN ('Firefox', 'Safari')
