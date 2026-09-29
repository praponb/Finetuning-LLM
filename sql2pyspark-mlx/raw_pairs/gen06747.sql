SELECT
    emp.email AS employee_email,
    emp.full_name AS employee_full_name,
    emp.is_remote AS employee_is_remote,
    mgr.dept_id AS manager_dept_id,
    mgr.salary AS manager_salary
FROM employees AS emp
LEFT JOIN employees AS mgr ON emp.manager_id = mgr.id
WHERE mgr.level IN ('L5', 'L1') AND emp.manager_id IN (6, 18, 32)
