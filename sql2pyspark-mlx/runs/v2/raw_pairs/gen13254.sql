WITH department_summary AS (
    SELECT dept_id, MAX(salary) - MIN(salary) AS salary_range, COUNT(DISTINCT salary) AS unique_depts
    FROM employees
    GROUP BY dept_id
)
SELECT d.budget, ds.salary_range, ds.unique_depts
FROM departments d
INNER JOIN department_summary ds ON d.id = ds.dept_id
WHERE ds.unique_depts >= 4;
