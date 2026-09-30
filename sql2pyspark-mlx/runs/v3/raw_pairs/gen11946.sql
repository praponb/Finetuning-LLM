SELECT
    e.id,
    e.hire_date AS employee_hire_date,
    m.manager_id AS manager_manager_id,
    m.is_remote AS manager_is_remote,
    m.dept_id AS manager_dept_id
FROM employees e
JOIN employees m ON e.manager_id = m.id
