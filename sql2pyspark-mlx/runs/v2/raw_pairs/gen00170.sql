SELECT id, dept_id, email FROM employees WHERE is_remote UNION ALL SELECT id, dept_id, email FROM employees WHERE manager_id IS NOT NULL;
