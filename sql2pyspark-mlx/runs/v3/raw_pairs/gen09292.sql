SELECT email FROM customers WHERE LOWER(TRIM(name)) = 'olivia garcia' UNION SELECT email FROM employees ORDER BY email DESC
