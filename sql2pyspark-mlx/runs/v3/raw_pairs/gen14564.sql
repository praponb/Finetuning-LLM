SELECT
    emp.job_title AS employee_job_title,
    emp.dept_id AS employee_dept_id,
    emp.full_name AS employee_full_name,
    mgr.is_remote AS manager_is_remote,
    mgr.email AS manager_email,
    mgr.level AS manager_level
FROM employees emp
JOIN employees mgr ON emp.manager_id = mgr.id
WHERE mgr.dept_id IN (12, 32, 39)
ORDER BY employee_full_name;
