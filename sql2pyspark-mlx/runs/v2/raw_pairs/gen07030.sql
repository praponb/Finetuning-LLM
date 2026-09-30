WITH employees_by_department AS (
    SELECT dept_id, COUNT(DISTINCT salary) AS unique_job_titles
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.location,
    d.budget,
    ebd.unique_job_titles
FROM departments d
INNER JOIN employees_by_department ebd ON d.id = ebd.dept_id
WHERE ebd.unique_job_titles >= 7;
