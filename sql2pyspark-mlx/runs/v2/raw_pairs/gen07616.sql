WITH department_stats AS (
    SELECT dept_id, COUNT(CASE WHEN level = 'L2' THEN 1 END) AS l2_count
    FROM employees
    WHERE salary > 140000
    GROUP BY dept_id
)
SELECT d.id, d.dept_name, ds.l2_count
FROM departments d
JOIN department_stats ds ON d.id = ds.dept_id
WHERE ds.l2_count >= 4
ORDER BY dept_name DESC, ds.l2_count DESC, id
LIMIT 20
