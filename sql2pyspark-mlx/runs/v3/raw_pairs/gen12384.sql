SELECT d.dept_name, MAX(e.salary) - MIN(e.salary) AS salary_range, COUNT(CASE WHEN e.level = 'L2' THEN 1 END) AS l2_count
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.id
GROUP BY d.dept_name
ORDER BY d.dept_name DESC;
