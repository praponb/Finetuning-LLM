SELECT
    emp.level AS employee_level,
    emp.email AS employee_email,
    mgr.job_title AS manager_job_title,
    mgr.is_remote AS manager_is_remote
FROM employees emp
JOIN employees mgr ON emp.manager_id = mgr.id;
