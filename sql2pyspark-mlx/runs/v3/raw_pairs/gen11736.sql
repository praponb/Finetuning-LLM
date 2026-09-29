SELECT
    e.id,
    e.job_title AS employee_job_title,
    e.salary AS employee_salary,
    m.salary AS manager_salary
FROM employees AS e
LEFT JOIN employees AS m ON e.manager_id = m.id;
