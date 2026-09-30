SELECT
    e.full_name employee_full_name,
    e.level employee_level,
    m.job_title manager_job_title,
    m.level manager_level
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
ORDER BY e.level DESC
