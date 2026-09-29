WITH EmployeesByDepartment AS (
    SELECT dept_id, MAX(salary) - MIN(salary) AS salary_range
    FROM employees
    WHERE job_title IN ('Intern', 'Engineer', 'Analyst')
    GROUP BY dept_id
)
SELECT d.id, d.location, E.salary_range
FROM departments d
LEFT JOIN EmployeesByDepartment E ON d.id = E.dept_id
ORDER BY location DESC
