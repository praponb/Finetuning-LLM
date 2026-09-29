SELECT e.manager_id, d.budget FROM employees e FULL OUTER JOIN departments d ON d.id = e.dept_id WHERE LOWER(e.full_name) LIKE '%rac%' ORDER BY budget;
