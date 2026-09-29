SELECT email FROM employees WHERE manager_id IS NOT NULL UNION ALL SELECT email FROM employees WHERE manager_id IN (3, 7, 16) ORDER BY email;
