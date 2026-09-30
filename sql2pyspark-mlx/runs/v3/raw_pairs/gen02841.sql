SELECT
    e.salary employee_salary,
    e.full_name employee_full_name,
    m.manager_id manager_manager_id,
    m.job_title manager_job_title,
    m.salary manager_salary
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
ORDER BY manager_salary;
