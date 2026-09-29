WITH employees_by_department AS (
    SELECT dept_id, COUNT(*) AS employee_count
    FROM employees
    WHERE full_name LIKE 'Mi%'
    GROUP BY dept_id
)
SELECT
    d.id,
    d.budget,
    d.dept_name,
    ebd.employee_count
FROM departments d
LEFT JOIN employees_by_department ebd ON d.id = ebd.dept_id
WHERE ebd.employee_count >= 7
ORDER BY employee_count DESC
