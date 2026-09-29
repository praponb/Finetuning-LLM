SELECT
    e.is_remote AS employee_is_remote,
    e.level AS employee_level,
    e.manager_id AS employee_manager_id,
    m.level AS manager_level,
    m.hire_date AS manager_hire_date,
    m.job_title AS manager_job_title
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE m.salary BETWEEN 71000 AND 110000
