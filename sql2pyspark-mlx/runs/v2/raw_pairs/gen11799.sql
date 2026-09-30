SELECT d.dept_name, MIN(e.salary) AS min_salary
FROM employees e
JOIN departments d ON d.id = e.dept_id
WHERE d.dept_name = 'Engineering'
GROUP BY d.dept_name
ORDER BY min_salary DESC, d.dept_name
LIMIT 10;
