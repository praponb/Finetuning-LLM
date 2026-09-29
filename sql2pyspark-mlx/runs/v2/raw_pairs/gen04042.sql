WITH department_summary AS (
    SELECT dept_id, MIN(salary) min_salary, SUM(salary) total_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.budget,
    ds.min_salary,
    ds.total_salary
FROM departments d
LEFT JOIN department_summary ds ON d.id = ds.dept_id
ORDER BY ds.total_salary
