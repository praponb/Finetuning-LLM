SELECT
    emp.is_remote AS employee_is_remote,
    emp.email AS employee_email,
    mgr.is_remote AS manager_is_remote,
    mgr.hire_date AS manager_hire_date
FROM employees emp
LEFT JOIN employees mgr ON emp.manager_id = mgr.id
WHERE mgr.level IN ('L5', 'L4')
