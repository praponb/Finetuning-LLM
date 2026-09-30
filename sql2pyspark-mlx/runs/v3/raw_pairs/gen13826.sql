SELECT
    emp.hire_date AS employee_hire_date,
    emp.is_remote AS employee_is_remote,
    mgr.job_title AS manager_job_title,
    mgr.is_remote AS manager_is_remote,
    COALESCE(emp.level, 'unassigned') AS level_filled
FROM employees emp
LEFT JOIN employees mgr ON emp.manager_id = mgr.id;
