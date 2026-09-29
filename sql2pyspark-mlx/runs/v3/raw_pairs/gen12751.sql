SELECT
    e.id,
    e.job_title AS employee_job_title,
    m.level AS manager_level,
    m.hire_date AS manager_hire_date,
    m.email AS manager_email,
    DATEDIFF(CURRENT_DATE(), m.hire_date) AS days_since_hire_date
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE NOT (e.level = 'L5' AND e.level = 'L5')
