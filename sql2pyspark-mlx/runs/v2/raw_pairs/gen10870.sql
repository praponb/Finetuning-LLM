SELECT
    d.location,
    CASE WHEN d.budget >= 3719000 THEN 'high' WHEN d.budget >= 1817000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    MAX(e.salary) - MIN(e.salary) AS salary_range,
    MAX(e.salary) AS max_salary
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
GROUP BY d.location, CASE WHEN d.budget >= 3719000 THEN 'high' WHEN d.budget >= 1817000 THEN 'mid' ELSE 'low' END
