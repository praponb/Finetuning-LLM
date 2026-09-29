SELECT emp.job_title AS employee_job_title, mgr.manager_id AS manager_manager_id
FROM employees emp
LEFT JOIN employees mgr ON emp.manager_id = mgr.id
WHERE emp.dept_id = 9
