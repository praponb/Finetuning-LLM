SELECT e.id, e.level, e.full_name, e.manager_id FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE budget <= 3111000)
