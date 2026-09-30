WITH EmployeesByDepartment AS (
    SELECT dept_id, MIN(salary) AS min_salary, SUM(CASE WHEN job_title = 'Director' THEN 1 ELSE 0 END) AS director_count
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.budget,
    d.location,
    E.min_salary,
    E.director_count
FROM departments d
LEFT JOIN EmployeesByDepartment E ON d.id = E.dept_id;
