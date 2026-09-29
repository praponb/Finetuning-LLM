SELECT id, is_active, signup_date, CASE WHEN country = 'JP' THEN 1 ELSE 0 END AS is_jp, SUBSTRING(name, 1, 2) AS name_prefix FROM customers WHERE credit_limit < 9400 ORDER BY name_prefix
