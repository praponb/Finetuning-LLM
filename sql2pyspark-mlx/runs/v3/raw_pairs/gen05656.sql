WITH DepartmentStats AS (
    SELECT dept_id, AVG(salary) AS avg_salary, SUM(salary) AS total_salary
    FROM employees
    WHERE dept_id IN (11, 25)
    GROUP BY dept_id
)
SELECT
    d.budget,
    d.location,
    D.avg_salary,
    D.total_salary
FROM departments d
INNER JOIN DepartmentStats D ON d.id = D.dept_id
ORDER BY D.avg_salary DESC;
