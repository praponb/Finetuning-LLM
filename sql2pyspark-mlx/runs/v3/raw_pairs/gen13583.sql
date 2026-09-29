SELECT
    e.id,
    e.job_title AS employee_job_title,
    e.salary AS employee_salary,
    m.job_title AS manager_job_title,
    m.level AS manager_level
FROM employees e
JOIN employees m ON e.manager_id = m.id
WHERE e.job_title IN ('Analyst', 'Designer', 'Intern') AND e.hire_date > '2025-02-01';
