WITH employees_by_department AS (
    SELECT dept_id, COUNT(*) AS total_count, MIN(hire_date) AS first_hire_date
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.dept_name,
    ebd.total_count,
    ebd.first_hire_date
FROM departments d
LEFT OUTER JOIN employees_by_department ebd ON d.id = ebd.dept_id
WHERE ebd.total_count > 6
ORDER BY id DESC;
