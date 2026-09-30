WITH department_stats AS (
    SELECT dept_id, ROUND((100 * SUM(CASE WHEN level = 'L3' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_l3, COUNT(DISTINCT salary) unique_depts
    FROM employees
    WHERE salary > 141000
    GROUP BY dept_id
)
SELECT d.location, ds.pct_l3, ds.unique_depts
FROM departments d
JOIN department_stats ds ON d.id = ds.dept_id
WHERE ds.pct_l3 > 71
ORDER BY pct_l3;
