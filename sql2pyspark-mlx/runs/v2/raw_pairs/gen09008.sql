SELECT departments.location, ROUND(AVG(employees.salary), 2) AS avg_salary, MAX(employees.hire_date) AS last_hire_date
FROM employees
INNER JOIN departments ON departments.id = employees.dept_id
WHERE employees.job_title IN ('Manager', 'Analyst', 'Designer')
GROUP BY departments.location
