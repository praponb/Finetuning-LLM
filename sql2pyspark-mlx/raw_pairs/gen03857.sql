SELECT
    CASE WHEN d.budget >= 4431000 THEN 'high' WHEN d.budget >= 1316000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    d.location,
    MAX(e.salary) - MIN(e.salary) AS salary_range,
    MAX(e.salary) AS max_salary
FROM employees e
RIGHT OUTER JOIN departments d ON e.dept_id = d.id
WHERE e.level = 'L5'
GROUP BY CASE WHEN d.budget >= 4431000 THEN 'high' WHEN d.budget >= 1316000 THEN 'mid' ELSE 'low' END, d.location
