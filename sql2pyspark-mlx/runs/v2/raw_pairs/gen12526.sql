WITH department_totals AS (
    SELECT dept_id, ROUND(AVG(salary), 2) AS avg_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.dept_name,
    d.location,
    dt.avg_salary
FROM departments d
INNER JOIN department_totals dt ON d.id = dt.dept_id
WHERE dt.avg_salary > 22;
