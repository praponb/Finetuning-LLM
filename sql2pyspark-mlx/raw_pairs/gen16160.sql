WITH employees_by_department AS (
    SELECT dept_id, COUNT(*) AS num_employees, COUNT(CASE WHEN job_title = 'Manager' THEN 1 END) AS manager_count
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.budget,
    d.dept_name,
    ebd.num_employees,
    ebd.manager_count
FROM departments d
LEFT JOIN employees_by_department ebd ON d.id = ebd.dept_id
WHERE ebd.manager_count >= 4
ORDER BY manager_count DESC;
