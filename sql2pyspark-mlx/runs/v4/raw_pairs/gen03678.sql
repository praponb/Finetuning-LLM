SELECT
    emp.level AS employee_level,
    emp.manager_id AS employee_manager_id,
    mgr.dept_id AS manager_dept_id,
    mgr.hire_date AS manager_hire_date,
    mgr.manager_id AS manager_manager_id
FROM employees AS emp
INNER JOIN employees AS mgr ON emp.manager_id = mgr.id
WHERE emp.job_title = 'Designer'
