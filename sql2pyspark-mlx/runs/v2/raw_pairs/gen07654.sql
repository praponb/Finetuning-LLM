SELECT
    e.is_remote AS employee_is_remote,
    e.hire_date AS employee_hire_date,
    e.job_title AS employee_job_title,
    m.level AS manager_level,
    m.salary AS manager_salary
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE e.hire_date > '2023-09-01';
