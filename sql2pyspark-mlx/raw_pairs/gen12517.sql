SELECT
    emp.salary AS employee_salary,
    emp.hire_date AS employee_hire_date,
    emp.email AS employee_email,
    mgr.manager_id AS manager_manager_id
FROM employees emp
LEFT JOIN employees mgr ON emp.manager_id = mgr.id
WHERE emp.hire_date > '2025-01-01'
ORDER BY manager_manager_id
