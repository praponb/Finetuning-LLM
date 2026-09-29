WITH department_summary AS (
    SELECT dept_id, MIN(salary) min_salary
    FROM employees
    WHERE NOT is_remote
    GROUP BY dept_id
)
SELECT d.id, d.dept_name, ds.min_salary
FROM departments d
INNER JOIN department_summary ds ON d.id = ds.dept_id
WHERE ds.min_salary >= 78000
