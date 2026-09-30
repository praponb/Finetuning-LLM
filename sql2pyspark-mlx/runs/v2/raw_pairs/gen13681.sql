SELECT
    emp.id,
    emp.full_name employee_full_name,
    emp.dept_id employee_dept_id,
    emp.job_title employee_job_title,
    mgr.email manager_email,
    mgr.job_title manager_job_title
FROM employees AS emp
LEFT JOIN employees AS mgr ON emp.manager_id = mgr.id
