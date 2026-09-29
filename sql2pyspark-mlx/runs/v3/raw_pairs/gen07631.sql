SELECT e.job_title, d.dept_name
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.id
WHERE d.location = 'Berlin' OR e.full_name NOT LIKE 'Dav%'
ORDER BY d.dept_name DESC
