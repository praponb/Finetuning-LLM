SELECT
    e.id,
    e.hire_date employee_hire_date,
    e.is_remote employee_is_remote,
    m.dept_id manager_dept_id
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE e.salary <= 101000
ORDER BY m.dept_id;
