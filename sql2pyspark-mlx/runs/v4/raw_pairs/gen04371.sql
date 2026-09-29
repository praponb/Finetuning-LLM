WITH employees_by_department AS (
    SELECT dept_id, SUM(CASE WHEN level = 'L4' THEN salary ELSE 0 END) AS l4_salary
    FROM employees
    GROUP BY dept_id
)
SELECT d.id, d.location, ebd.l4_salary
FROM departments d
LEFT JOIN employees_by_department ebd ON d.id = ebd.dept_id
WHERE ebd.l4_salary >= 66
ORDER BY d.location DESC;
