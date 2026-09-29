SELECT full_name, hire_date FROM employees WHERE manager_id IN (2, 27) UNION SELECT full_name, hire_date FROM employees WHERE is_remote;
