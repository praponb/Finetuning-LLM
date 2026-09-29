SELECT
    e.id,
    e.hire_date AS employee_hire_date,
    e.level AS employee_level,
    m.level AS manager_level
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
WHERE e.job_title <> 'Designer' OR m.hire_date < '2024-07-01'
ORDER BY employee_level
