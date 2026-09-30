SELECT
    e.id,
    e.full_name AS employee_full_name,
    m.email AS manager_email,
    m.job_title AS manager_job_title,
    m.full_name AS manager_full_name
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
ORDER BY 3, e.id DESC
LIMIT 5
