SELECT name FROM customers WHERE LOWER(TRIM(name)) = 'ivy chen' UNION ALL SELECT full_name FROM employees ORDER BY name;
