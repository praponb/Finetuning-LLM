SELECT id, manager_id, email FROM employees WHERE hire_date <= '2025-09-01' UNION SELECT id, manager_id, email FROM employees WHERE salary BETWEEN 118000 AND 127000 ORDER BY manager_id
