SELECT e.id, e.is_remote, d.dept_name FROM employees e LEFT JOIN departments d ON e.dept_id = d.id WHERE e.level IS NOT NULL ORDER BY e.id;
