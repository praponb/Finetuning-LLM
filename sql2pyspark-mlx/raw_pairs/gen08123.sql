SELECT
    emp.id,
    emp.level AS employee_level,
    mgr.full_name AS manager_full_name,
    IF(mgr.is_remote, 'Yes', 'No') AS is_remote_label
FROM employees emp
INNER JOIN employees mgr ON emp.manager_id = mgr.id
WHERE mgr.dept_id IN (18, 31, 32, 36)
ORDER BY mgr.full_name, is_remote_label DESC
