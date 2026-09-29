SELECT
    e.id,
    e.full_name employee_full_name,
    e.dept_id employee_dept_id,
    e.hire_date employee_hire_date,
    m.job_title manager_job_title,
    m.is_remote manager_is_remote
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE m.full_name LIKE 'Gra%';
