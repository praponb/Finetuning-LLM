SELECT
    emp.id,
    emp.dept_id AS employee_dept_id,
    mgr.email AS manager_email,
    mgr.level AS manager_level,
    mgr.dept_id AS manager_dept_id
FROM employees emp
LEFT OUTER JOIN employees mgr ON emp.manager_id = mgr.id
WHERE mgr.dept_id IN (9, 10, 36)
