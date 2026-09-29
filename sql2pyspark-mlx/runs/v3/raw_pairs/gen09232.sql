SELECT
    emp.id,
    emp.email AS employee_email,
    emp.full_name AS employee_full_name,
    emp.salary AS employee_salary,
    mgr.dept_id AS manager_dept_id,
    mgr.job_title AS manager_job_title,
    mgr.manager_id AS manager_manager_id
FROM employees emp
INNER JOIN employees mgr ON emp.manager_id = mgr.id
WHERE emp.level = 'L5'
ORDER BY employee_full_name DESC;
