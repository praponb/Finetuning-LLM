WITH DepartmentSummary AS (
    SELECT dept_id, MAX(hire_date) AS last_hire_date
    FROM employees
    WHERE NOT is_remote
    GROUP BY dept_id
)
SELECT d.dept_name, D.last_hire_date
FROM departments d
LEFT JOIN DepartmentSummary D ON d.id = D.dept_id
ORDER BY D.last_hire_date, dept_name;
