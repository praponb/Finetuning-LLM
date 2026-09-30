SELECT
    e.id,
    e.job_title employee_job_title,
    e.hire_date employee_hire_date,
    m.level manager_level,
    m.hire_date manager_hire_date
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
ORDER BY employee_job_title DESC, id
LIMIT 10
