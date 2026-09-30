WITH department_stats AS (
    SELECT dept_id, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY dept_id
)
SELECT d.id, d.location, ds.avg_salary
FROM departments d
JOIN department_stats ds ON d.id = ds.dept_id
