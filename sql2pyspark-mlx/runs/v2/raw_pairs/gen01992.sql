SELECT e.id, e.full_name, d.dept_name
FROM employees e
INNER JOIN departments d ON d.id = e.dept_id
WHERE e.full_name IS NULL OR e.job_title IN ('Engineer', 'Manager')
