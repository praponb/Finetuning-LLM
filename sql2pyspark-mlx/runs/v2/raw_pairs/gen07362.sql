SELECT
    emp.id,
    emp.email AS employee_email,
    emp.manager_id AS employee_manager_id,
    mgr.job_title AS manager_job_title,
    CASE WHEN mgr.level IN ('L1', 'L3') THEN 'Priority' ELSE 'Other' END AS level_group
FROM employees AS emp
LEFT OUTER JOIN employees AS mgr ON emp.manager_id = mgr.id
WHERE mgr.salary > 68000
ORDER BY employee_manager_id DESC, level_group DESC;
