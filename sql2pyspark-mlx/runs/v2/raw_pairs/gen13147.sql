SELECT
    emp.id,
    emp.email AS employee_email,
    emp.salary AS employee_salary,
    mgr.manager_id AS manager_manager_id
FROM employees emp
INNER JOIN employees mgr ON emp.manager_id = mgr.id
WHERE mgr.level <> 'L3'
