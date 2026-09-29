WITH DepartmentStats AS (
    SELECT dept_id, SUM(CASE WHEN level = 'L5' THEN 1 ELSE 0 END) AS l5_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.dept_name,
    d.budget,
    D.l5_count,
    D.level_values
FROM departments d
INNER JOIN DepartmentStats D ON d.id = D.dept_id
WHERE D.l5_count > 27
ORDER BY l5_count DESC
