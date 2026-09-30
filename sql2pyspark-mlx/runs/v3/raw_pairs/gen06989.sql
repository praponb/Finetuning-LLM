SELECT
    emp.id,
    emp.hire_date AS employee_hire_date,
    mgr.is_remote AS manager_is_remote,
    mgr.level AS manager_level,
    mgr.full_name AS manager_full_name
FROM employees emp
INNER JOIN employees mgr ON emp.manager_id = mgr.id
