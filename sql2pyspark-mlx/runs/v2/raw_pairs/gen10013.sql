SELECT d.location, ROUND((100 * SUM(CASE WHEN e.job_title = 'Intern' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_intern
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.id
WHERE d.budget >= 1081000
GROUP BY d.location
HAVING pct_intern > 28;
