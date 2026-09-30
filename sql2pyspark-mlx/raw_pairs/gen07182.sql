SELECT d.budget, e.hire_date, e.level, e.salary FROM departments d LEFT JOIN employees e ON e.dept_id = d.id WHERE e.dept_id IN (10, 14, 20, 38) AND e.full_name LIKE '%Chen%'
