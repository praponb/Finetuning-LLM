SELECT
    e.id,
    e.full_name employee_full_name,
    e.job_title employee_job_title,
    e.level employee_level,
    m.email manager_email,
    m.salary manager_salary,
    m.is_remote manager_is_remote,
    LAST_DAY(e.hire_date) hire_date_month_end
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
WHERE m.is_remote AND e.is_remote
ORDER BY hire_date_month_end
