WITH department_stats AS (
    SELECT dept_id, SUM(CASE WHEN level = 'L5' THEN 1 ELSE 0 END) AS l5_count
    FROM employees
    GROUP BY dept_id
)
SELECT d.dept_name, d.budget, ds.l5_count
FROM departments d
INNER JOIN department_stats ds ON d.id = ds.dept_id;
