SELECT d.dept_name, COUNT(DISTINCT e.salary) AS unique_levels
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.id
WHERE d.location IS NOT NULL
GROUP BY d.dept_name;
