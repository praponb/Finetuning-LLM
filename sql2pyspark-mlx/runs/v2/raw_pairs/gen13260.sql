SELECT
    e.salary AS employee_salary,
    e.job_title AS employee_job_title,
    m.email AS manager_email,
    m.level AS manager_level
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
WHERE NOT e.is_remote AND e.hire_date BETWEEN '2024-01-01' AND '2024-08-01';
