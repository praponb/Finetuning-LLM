SELECT
    e.manager_id employee_manager_id,
    e.full_name employee_full_name,
    e.salary employee_salary,
    m.job_title manager_job_title,
    m.salary manager_salary
FROM employees e
JOIN employees m ON e.manager_id = m.id
WHERE e.level = 'L3'
ORDER BY e.full_name ASC;
