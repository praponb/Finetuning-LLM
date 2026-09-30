SELECT
    emp.id,
    emp.hire_date employee_hire_date,
    emp.full_name employee_full_name,
    emp.job_title employee_job_title,
    mgr.level manager_level,
    mgr.is_remote manager_is_remote
FROM employees AS emp
JOIN employees AS mgr ON emp.manager_id = mgr.id
