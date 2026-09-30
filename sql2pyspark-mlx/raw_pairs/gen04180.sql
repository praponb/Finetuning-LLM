WITH employees_by_department AS (
    SELECT dept_id, SUM(CASE WHEN level = 'L5' THEN 1 ELSE 0 END) AS l5_count, MAX(hire_date) AS last_hire_date
    FROM employees
    GROUP BY dept_id
)
SELECT d.budget, ebd.l5_count, ebd.last_hire_date
FROM departments d
INNER JOIN employees_by_department ebd ON d.id = ebd.dept_id
WHERE ebd.l5_count > 42
