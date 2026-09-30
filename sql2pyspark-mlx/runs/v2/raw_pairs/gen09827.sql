SELECT e.level, e.is_remote, d.location, d.dept_name FROM employees e INNER JOIN departments d ON e.dept_id = d.id WHERE e.full_name IS NOT NULL OR e.salary < 136000 ORDER BY d.location
