WITH DepartmentTotals AS (
    SELECT dept_id, ROUND((100 * SUM(CASE WHEN level = 'L5' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_l5
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.budget,
    d.location,
    D.pct_l5
FROM departments d
LEFT JOIN DepartmentTotals D ON d.id = D.dept_id
WHERE D.pct_l5 > 77
ORDER BY id DESC, d.location DESC;
