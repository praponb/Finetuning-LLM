WITH department_summary AS (
    SELECT dept_id, SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END) designer_count, MIN(salary) min_salary
    FROM employees
    WHERE is_remote
    GROUP BY dept_id
)
SELECT
    d.location,
    d.dept_name,
    ds.designer_count,
    ds.min_salary
FROM departments d
INNER JOIN department_summary ds ON d.id = ds.dept_id
WHERE ds.designer_count > 45
ORDER BY d.location
