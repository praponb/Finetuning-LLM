SELECT
    emp.id,
    emp.hire_date AS employee_hire_date,
    emp.full_name AS employee_full_name,
    emp.email AS employee_email,
    mgr.is_remote AS manager_is_remote,
    mgr.level AS manager_level
FROM employees emp
LEFT JOIN employees mgr ON emp.manager_id = mgr.id;
