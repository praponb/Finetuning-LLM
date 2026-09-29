SELECT
    emp.id,
    emp.email AS employee_email,
    emp.dept_id AS employee_dept_id,
    mgr.is_remote AS manager_is_remote,
    mgr.level AS manager_level,
    mgr.manager_id AS manager_manager_id
FROM employees emp
LEFT JOIN employees mgr ON emp.manager_id = mgr.id
WHERE mgr.job_title = 'Designer' AND mgr.manager_id IN (16, 21, 31);
