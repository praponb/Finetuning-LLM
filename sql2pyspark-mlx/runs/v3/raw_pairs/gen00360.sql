WITH department_stats AS (
    SELECT dept_id, SUM(CASE WHEN job_title = 'Designer' THEN salary ELSE 0 END) AS designer_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.location,
    d.budget,
    ds.designer_salary
FROM departments d
LEFT JOIN department_stats ds ON d.id = ds.dept_id
WHERE ds.designer_salary > 43;
