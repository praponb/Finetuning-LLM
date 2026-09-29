WITH department_totals AS (
    SELECT dept_id, ROUND(AVG(salary), 2) AS avg_salary, MAX(salary) AS max_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.location,
    dt.avg_salary,
    dt.max_salary
FROM departments d
INNER JOIN department_totals dt ON d.id = dt.dept_id
ORDER BY avg_salary DESC;
