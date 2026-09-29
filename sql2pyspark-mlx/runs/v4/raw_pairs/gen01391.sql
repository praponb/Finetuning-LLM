SELECT e.level, d.budget FROM employees e LEFT OUTER JOIN departments d ON d.id = e.dept_id ORDER BY level ASC, d.budget DESC;
