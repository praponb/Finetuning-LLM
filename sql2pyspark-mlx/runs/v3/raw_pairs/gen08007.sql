WITH DepartmentTotals AS (
    SELECT dept_id, SUM(CASE WHEN level = 'L1' THEN 1 ELSE 0 END) AS l1_count
    FROM employees
    GROUP BY dept_id
)
SELECT departments.id, departments.location, DepartmentTotals.l1_count
FROM departments
JOIN DepartmentTotals ON departments.id = DepartmentTotals.dept_id
ORDER BY l1_count, id
LIMIT 5
