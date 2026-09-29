SELECT
    e.id,
    e.is_remote AS employee_is_remote,
    e.salary AS employee_salary,
    m.email AS manager_email,
    m.manager_id AS manager_manager_id,
    m.job_title AS manager_job_title
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE m.email IS NOT NULL
ORDER BY e.salary DESC
