WITH DepartmentTotals AS (
    SELECT dept_id, COUNT(CASE WHEN level = 'L2' THEN 1 END) AS l2_count
    FROM employees
    WHERE salary > 138000
    GROUP BY dept_id
)
SELECT d.dept_name, D.l2_count
FROM departments d
LEFT JOIN DepartmentTotals D ON d.id = D.dept_id
WHERE D.l2_count >= 7
ORDER BY dept_name ASC;
