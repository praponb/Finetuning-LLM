SELECT
    emp.email employee_email,
    emp.is_remote employee_is_remote,
    emp.salary employee_salary,
    mgr.manager_id manager_manager_id,
    mgr.salary manager_salary,
    mgr.email manager_email
FROM employees emp
LEFT JOIN employees mgr ON emp.manager_id = mgr.id
WHERE mgr.email IS NULL OR emp.salary < 91000
ORDER BY mgr.manager_id DESC
