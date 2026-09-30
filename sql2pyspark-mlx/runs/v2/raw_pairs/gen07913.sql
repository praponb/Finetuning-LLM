WITH department_stats AS (
    SELECT dept_id, COUNT(DISTINCT salary) AS unique_managers, ROUND(STDDEV(salary), 2) AS stddev_salary
    FROM employees
    WHERE job_title IS NULL
    GROUP BY dept_id
)
SELECT d.dept_name, ds.unique_managers, ds.stddev_salary
FROM departments d
LEFT JOIN department_stats ds ON d.id = ds.dept_id
