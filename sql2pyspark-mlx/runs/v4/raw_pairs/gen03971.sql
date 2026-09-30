SELECT d.location, d.dept_name, ROUND((100 * SUM(CASE WHEN e.job_title = 'Engineer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_engineer
FROM employees e
RIGHT OUTER JOIN departments d ON e.dept_id = d.id
GROUP BY d.location, d.dept_name
HAVING ROUND((100 * SUM(CASE WHEN e.job_title = 'Engineer' THEN 1 ELSE 0 END)) / COUNT(*), 1) >= 47
ORDER BY pct_engineer DESC, location, dept_name
LIMIT 10
