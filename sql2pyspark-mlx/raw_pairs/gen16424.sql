SELECT CASE WHEN d.budget >= 3510000 THEN 'high' WHEN d.budget >= 716000 THEN 'mid' ELSE 'low' END AS budget_bucket, d.dept_name, COUNT(DISTINCT e.salary) AS unique_managers
FROM employees e
FULL OUTER JOIN departments d ON e.dept_id = d.id
GROUP BY CASE WHEN d.budget >= 3510000 THEN 'high' WHEN d.budget >= 716000 THEN 'mid' ELSE 'low' END, d.dept_name
