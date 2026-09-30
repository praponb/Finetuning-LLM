SELECT hire_date FROM employees WHERE LENGTH(full_name) >= 7 UNION SELECT hire_date FROM employees WHERE full_name IS NOT NULL;
