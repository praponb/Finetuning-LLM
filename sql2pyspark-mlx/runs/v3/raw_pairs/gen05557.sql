SELECT
    e.job_title AS employee_job_title,
    e.manager_id AS employee_manager_id,
    e.is_remote AS employee_is_remote,
    m.is_remote AS manager_is_remote
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE m.salary < 77000 AND e.level = 'L2'
