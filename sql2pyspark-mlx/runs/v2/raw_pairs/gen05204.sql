SELECT d.location, ROUND(AVG(e.salary), 2) AS avg_salary, ROUND(STDDEV(e.salary), 2) AS stddev_salary
FROM employees e
INNER JOIN departments d ON d.id = e.dept_id
WHERE d.dept_name IS NOT NULL
GROUP BY d.location
ORDER BY 2 DESC
