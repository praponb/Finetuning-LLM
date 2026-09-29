WITH DepartmentStats AS (
    SELECT dept_id, MIN(salary) AS min_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.dept_name,
    d.budget,
    D.min_salary
FROM departments d
JOIN DepartmentStats D ON d.id = D.dept_id
WHERE D.min_salary > 106000;
