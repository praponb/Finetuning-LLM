SELECT
    e.id,
    e.email AS employee_email,
    e.salary AS employee_salary,
    e.job_title AS employee_job_title,
    m.salary AS manager_salary,
    DATEDIFF(CURRENT_DATE(), m.hire_date) AS days_since_hire_date
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
ORDER BY manager_salary DESC, id ASC
LIMIT 10;
