SELECT email, 'customer' AS source FROM customers UNION ALL SELECT email, 'employee' AS source FROM employees WHERE is_remote = FALSE ORDER BY email DESC
