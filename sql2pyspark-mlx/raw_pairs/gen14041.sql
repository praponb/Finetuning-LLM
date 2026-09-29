SELECT e.hire_date, d.budget FROM employees e LEFT OUTER JOIN departments d ON e.dept_id = d.id
