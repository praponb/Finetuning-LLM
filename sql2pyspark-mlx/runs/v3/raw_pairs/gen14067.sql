WITH department_stats AS (
    SELECT dept_id, MAX(salary) AS max_salary
    FROM employees
    WHERE NOT is_remote
    GROUP BY dept_id
)
SELECT d.budget, d.dept_name, ds.max_salary
FROM departments d
LEFT JOIN department_stats ds ON d.id = ds.dept_id
WHERE ds.max_salary >= 61000
