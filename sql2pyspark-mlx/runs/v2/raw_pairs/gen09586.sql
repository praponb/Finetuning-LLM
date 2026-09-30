SELECT d.dept_name, d.location, ROUND(STDDEV(e.salary), 2) AS stddev_salary
FROM employees e
FULL OUTER JOIN departments d ON e.dept_id = d.id
WHERE d.budget <= 1455000
GROUP BY d.dept_name, d.location
HAVING ROUND(STDDEV(e.salary), 2) > 28
ORDER BY stddev_salary, dept_name;
