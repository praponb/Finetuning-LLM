SELECT CASE WHEN d.budget >= 3907000 THEN 'high' WHEN d.budget >= 1910000 THEN 'mid' ELSE 'low' END AS budget_bucket, ROUND(STDDEV(e.salary), 2) AS stddev_salary, MAX(e.salary) - MIN(e.salary) AS salary_range
FROM employees e
INNER JOIN departments d ON d.id = e.dept_id
GROUP BY CASE WHEN d.budget >= 3907000 THEN 'high' WHEN d.budget >= 1910000 THEN 'mid' ELSE 'low' END
ORDER BY salary_range DESC, budget_bucket
LIMIT 50;
