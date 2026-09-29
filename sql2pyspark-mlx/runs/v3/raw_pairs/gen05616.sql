WITH DepartmentTotals AS (
    SELECT dept_id, COUNT(DISTINCT salary) AS unique_job_titles
    FROM employees
    WHERE manager_id = 36
    GROUP BY dept_id
)
SELECT d.location, d.dept_name, D.unique_job_titles
FROM departments d
INNER JOIN DepartmentTotals D ON d.id = D.dept_id
ORDER BY d.dept_name;
