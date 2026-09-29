SELECT
    emp.is_remote AS employee_is_remote,
    mgr.salary AS manager_salary,
    mgr.hire_date AS manager_hire_date,
    mgr.is_remote AS manager_is_remote
FROM employees emp
LEFT JOIN employees mgr ON emp.manager_id = mgr.id
WHERE emp.email NOT LIKE '%@startup.io' OR emp.manager_id IS NULL
ORDER BY mgr.salary DESC;
