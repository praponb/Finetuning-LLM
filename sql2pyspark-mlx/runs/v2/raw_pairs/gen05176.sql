SELECT d.dept_name, CASE WHEN d.budget >= 3316000 THEN 'high' WHEN d.budget >= 1961000 THEN 'mid' ELSE 'low' END AS budget_bucket, MAX(e.salary) AS max_salary
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
GROUP BY d.dept_name, CASE WHEN d.budget >= 3316000 THEN 'high' WHEN d.budget >= 1961000 THEN 'mid' ELSE 'low' END
HAVING max_salary >= 141000
ORDER BY dept_name, budget_bucket
