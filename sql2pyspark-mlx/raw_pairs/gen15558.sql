SELECT s.* FROM departments s WHERE EXISTS (SELECT 1 FROM employees c WHERE c.dept_id = s.id AND c.dept_id IN (30, 35, 39));
