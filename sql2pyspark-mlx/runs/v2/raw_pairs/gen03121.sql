WITH department_stats AS (
    SELECT dept_id, COUNT(CASE WHEN job_title = 'Designer' THEN 1 END) AS designer_count
    FROM employees
    GROUP BY dept_id
)
SELECT d.dept_name, ds.designer_count
FROM departments d
INNER JOIN department_stats ds ON d.id = ds.dept_id
