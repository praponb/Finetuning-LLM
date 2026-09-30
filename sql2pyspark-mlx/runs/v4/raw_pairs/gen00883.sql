SELECT departments.location, ROUND(AVG(employees.salary), 2) AS avg_salary
FROM employees
INNER JOIN departments ON employees.dept_id = departments.id
GROUP BY departments.location
HAVING ROUND(AVG(employees.salary), 2) >= 53;
