SELECT
    e.id,
    e.email AS employee_email,
    e.manager_id AS employee_manager_id,
    e.hire_date AS employee_hire_date,
    m.full_name AS manager_full_name,
    m.job_title AS manager_job_title
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE m.salary > 106000
ORDER BY e.id, employee_hire_date
LIMIT 50
