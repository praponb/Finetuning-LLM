SELECT
    e.id,
    e.job_title AS employee_job_title,
    e.email AS employee_email,
    m.job_title AS manager_job_title,
    m.hire_date AS manager_hire_date,
    m.salary AS manager_salary
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
ORDER BY id DESC
LIMIT 5;
