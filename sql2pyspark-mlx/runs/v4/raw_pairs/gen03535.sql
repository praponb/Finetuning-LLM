SELECT s.* FROM departments s WHERE NOT EXISTS (SELECT 1 FROM employees c WHERE c.dept_id = s.id AND c.manager_id IS NOT NULL)
