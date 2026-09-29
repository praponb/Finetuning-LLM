WITH employees_by_department AS (
    SELECT dept_id, ROUND((100 * SUM(CASE WHEN level = 'L1' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l1, COUNT(DISTINCT salary) AS unique_depts
    FROM employees
    WHERE UPPER(full_name) LIKE '%MMA%'
    GROUP BY dept_id
)
SELECT
    d.id,
    d.dept_name,
    ebd.pct_l1,
    ebd.unique_depts
FROM departments d
LEFT JOIN employees_by_department ebd ON d.id = ebd.dept_id
WHERE ebd.pct_l1 >= 45
ORDER BY unique_depts DESC
