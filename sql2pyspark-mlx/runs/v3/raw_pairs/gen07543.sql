SELECT
    e.id,
    e.manager_id AS employee_manager_id,
    e.job_title AS employee_job_title,
    e.is_remote AS employee_is_remote,
    m.job_title AS manager_job_title,
    m.level AS manager_level,
    m.full_name AS manager_full_name,
    CEIL(e.salary) AS salary_whole
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE m.level NOT IN ('L5', 'L1', 'L3')
