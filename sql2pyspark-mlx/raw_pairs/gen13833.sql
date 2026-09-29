WITH department_totals AS (
    SELECT dept_id, COUNT(DISTINCT salary) AS unique_levels, ROUND(AVG(salary), 2) AS avg_salary
    FROM employees
    WHERE NOT is_remote
    GROUP BY dept_id
)
SELECT
    d.id,
    d.location,
    d.dept_name,
    dt.unique_levels,
    dt.avg_salary
FROM departments d
LEFT OUTER JOIN department_totals dt ON d.id = dt.dept_id
WHERE dt.unique_levels >= 7
