SELECT email FROM customers UNION ALL SELECT email FROM employees WHERE email LIKE '%@startup.io' ORDER BY email
