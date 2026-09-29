SELECT
    departments.id,
    departments.location,
    COUNT(employees.id) AS employee_count,
    MIN(employees.salary) AS min_salary
FROM departments
INNER JOIN employees ON departments.id = employees.dept_id
WHERE employees.manager_id IN (8, 9, 26, 28)
GROUP BY departments.id, departments.location
ORDER BY min_salary DESC;
