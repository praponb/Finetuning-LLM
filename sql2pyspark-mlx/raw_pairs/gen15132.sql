SELECT
    e.level AS employee_level,
    e.dept_id AS employee_dept_id,
    e.hire_date AS employee_hire_date,
    m.full_name AS manager_full_name,
    m.hire_date AS manager_hire_date
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
ORDER BY 2 DESC, 5
