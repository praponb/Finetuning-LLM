SELECT
    e.id,
    e.job_title AS employee_job_title,
    e.hire_date AS employee_hire_date,
    e.level AS employee_level,
    m.manager_id AS manager_manager_id,
    m.level AS manager_level,
    m.hire_date AS manager_hire_date
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE m.manager_id IN (5, 36) AND m.dept_id IN (6, 15, 19);
