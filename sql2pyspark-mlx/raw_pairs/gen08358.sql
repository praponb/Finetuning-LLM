SELECT email, 'customer' AS kind FROM customers WHERE is_active UNION SELECT email, 'employee' AS kind FROM employees ORDER BY email;
