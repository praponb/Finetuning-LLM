SELECT
    e.id,
    e.job_title AS employee_job_title,
    e.hire_date AS employee_hire_date,
    m.email AS manager_email
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
