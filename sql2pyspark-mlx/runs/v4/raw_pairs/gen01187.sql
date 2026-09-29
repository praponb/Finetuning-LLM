WITH department_summary AS (
    SELECT dept_id, COUNT(DISTINCT salary) AS unique_levels, COUNT(*) AS cnt
    FROM employees
    GROUP BY dept_id
)
SELECT d.budget, ds.unique_levels, ds.cnt
FROM departments d
JOIN department_summary ds ON d.id = ds.dept_id;
