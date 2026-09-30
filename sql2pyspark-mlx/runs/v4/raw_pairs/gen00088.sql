SELECT
    emp.id,
    emp.is_remote AS employee_is_remote,
    emp.hire_date AS employee_hire_date,
    mgr.hire_date AS manager_hire_date,
    mgr.is_remote AS manager_is_remote,
    mgr.dept_id AS manager_dept_id
FROM employees emp
LEFT JOIN employees mgr ON emp.manager_id = mgr.id
WHERE emp.level != 'L3'
ORDER BY manager_dept_id DESC, manager_hire_date
