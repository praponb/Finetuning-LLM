SELECT
    emp.job_title AS employee_job_title,
    emp.manager_id AS employee_manager_id,
    emp.full_name AS employee_full_name,
    mgr.full_name AS manager_full_name,
    CONCAT(mgr.job_title, ', ', mgr.level) AS job_title_level
FROM employees emp
INNER JOIN employees mgr ON emp.manager_id = mgr.id
