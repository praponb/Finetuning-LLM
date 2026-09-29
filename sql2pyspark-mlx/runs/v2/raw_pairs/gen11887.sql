SELECT
    e.salary employee_salary,
    e.email employee_email,
    e.hire_date employee_hire_date,
    m.job_title manager_job_title
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE e.hire_date <= '2025-02-04'
ORDER BY employee_hire_date;
