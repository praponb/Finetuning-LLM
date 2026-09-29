SELECT is_active, MIN(signup_date) AS first_signup_date, SUM(CASE WHEN country = 'US' THEN 1 ELSE 0 END) AS us_count FROM customers WHERE email IS NOT NULL AND LENGTH(name) > 7 GROUP BY 1
