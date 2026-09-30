SELECT dept_id, manager_id FROM employees WHERE salary >= 145000 UNION ALL SELECT dept_id, manager_id FROM employees WHERE level IN ('L3', 'L4', 'L5') ORDER BY dept_id;
