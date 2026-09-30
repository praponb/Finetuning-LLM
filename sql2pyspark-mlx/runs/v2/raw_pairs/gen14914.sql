SELECT d.budget, d.location, d.dept_name, e.hire_date, e.level, e.dept_id FROM departments d LEFT OUTER JOIN employees e ON d.id = e.dept_id WHERE d.location != 'Berlin' ORDER BY level ASC
