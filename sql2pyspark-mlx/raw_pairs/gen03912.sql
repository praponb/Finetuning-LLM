SELECT
    emp.id,
    emp.dept_id AS employee_dept_id,
    emp.email AS employee_email,
    mgr.is_remote AS manager_is_remote,
    mgr.email AS manager_email,
    mgr.job_title AS manager_job_title
FROM employees AS emp
INNER JOIN employees AS mgr ON emp.manager_id = mgr.id
WHERE mgr.level IN ('L4', 'L3');
