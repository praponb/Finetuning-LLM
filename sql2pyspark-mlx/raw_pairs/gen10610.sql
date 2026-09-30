WITH EmployeesByDepartment AS (
    SELECT dept_id, MIN(salary) AS min_salary
    FROM employees
    WHERE salary < 145000
    GROUP BY dept_id
)
SELECT d.dept_name, E.min_salary
FROM departments d
LEFT OUTER JOIN EmployeesByDepartment E ON d.id = E.dept_id
WHERE E.min_salary > 144000
ORDER BY 2, dept_name;
