WITH department_summary AS (
    SELECT dept_id, COUNT(DISTINCT salary) AS unique_depts, MAX(hire_date) AS last_hire_date
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.location,
    d.budget,
    ds.unique_depts,
    ds.last_hire_date
FROM departments d
JOIN department_summary ds ON d.id = ds.dept_id
WHERE ds.unique_depts > 7
