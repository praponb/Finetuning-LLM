SELECT e.id, e.level, e.hire_date, e.full_name FROM employees e WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE budget <= 1876000);
