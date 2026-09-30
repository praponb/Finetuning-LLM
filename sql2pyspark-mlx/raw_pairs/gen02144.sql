SELECT 'customer' AS record_type, email FROM customers WHERE NOT is_active UNION SELECT 'employee' AS record_type, email FROM employees;
