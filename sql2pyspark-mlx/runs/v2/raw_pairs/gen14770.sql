SELECT
    emp.id,
    emp.manager_id AS employee_manager_id,
    emp.is_remote AS employee_is_remote,
    mgr.manager_id AS manager_manager_id,
    mgr.hire_date AS manager_hire_date,
    LOWER(emp.email) AS email_lower
FROM employees emp
INNER JOIN employees mgr ON emp.manager_id = mgr.id
WHERE emp.is_remote = TRUE
