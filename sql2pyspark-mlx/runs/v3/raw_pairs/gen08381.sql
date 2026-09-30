SELECT
    e.job_title AS employee_job_title,
    e.hire_date AS employee_hire_date,
    e.is_remote AS employee_is_remote,
    m.full_name AS manager_full_name,
    m.email AS manager_email
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
WHERE e.is_remote
ORDER BY manager_email ASC
