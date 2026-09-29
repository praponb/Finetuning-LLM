WITH employees_by_department AS (
    SELECT dept_id, COUNT(*) AS cnt
    FROM employees
    WHERE email IS NOT NULL
    GROUP BY dept_id
)
SELECT
    d.id,
    d.dept_name,
    d.location,
    ebd.cnt
FROM departments d
INNER JOIN employees_by_department ebd ON d.id = ebd.dept_id
WHERE ebd.cnt > 8
ORDER BY d.dept_name ASC;
