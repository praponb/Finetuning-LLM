WITH employees_by_department AS (
    SELECT dept_id, SUM(CASE WHEN job_title = 'Intern' THEN 1 ELSE 0 END) AS intern_count
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.location,
    d.dept_name,
    ebd.intern_count
FROM departments d
JOIN employees_by_department ebd ON d.id = ebd.dept_id
WHERE ebd.intern_count >= 64
ORDER BY d.dept_name
