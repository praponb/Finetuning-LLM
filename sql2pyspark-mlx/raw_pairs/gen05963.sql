SELECT 'customer' AS origin, country FROM customers WHERE signup_date <= '2024-01-01' UNION ALL SELECT 'session' AS origin, country FROM sessions ORDER BY origin;
