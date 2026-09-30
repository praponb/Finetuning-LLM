SELECT d.location, COUNT(DISTINCT e.salary) AS unique_levels, ROUND(AVG(e.salary), 2) AS avg_salary
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.id
WHERE d.budget <= 1716000
GROUP BY d.location;
