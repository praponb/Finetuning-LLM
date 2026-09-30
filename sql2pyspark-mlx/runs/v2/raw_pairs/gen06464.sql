SELECT
    emp.id,
    emp.manager_id employee_manager_id,
    emp.dept_id employee_dept_id,
    emp.full_name employee_full_name,
    mgr.dept_id manager_dept_id,
    mgr.level manager_level,
    mgr.email manager_email
FROM employees AS emp
INNER JOIN employees AS mgr ON emp.manager_id = mgr.id;
