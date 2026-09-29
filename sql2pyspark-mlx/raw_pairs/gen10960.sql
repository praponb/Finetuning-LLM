WITH employees_by_department AS (
    SELECT dept_id, COUNT(DISTINCT salary) AS unique_depts
    FROM employees
    WHERE NOT is_remote
    GROUP BY dept_id
)
SELECT d.location, ebd.unique_depts
FROM departments d
INNER JOIN employees_by_department ebd ON d.id = ebd.dept_id
WHERE ebd.unique_depts >= 7
ORDER BY d.location DESC
