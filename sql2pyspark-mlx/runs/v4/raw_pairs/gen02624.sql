SELECT e.id, e.hire_date, e.level FROM employees e WHERE MONTH(e.hire_date) IN (2, 5, 10) AND e.dept_id NOT IN (SELECT id FROM departments WHERE dept_name IN ('Legal', 'Marketing'));
