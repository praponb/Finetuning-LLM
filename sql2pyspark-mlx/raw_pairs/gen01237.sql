SELECT
    e.id,
    e.hire_date AS employee_hire_date,
    e.full_name AS employee_full_name,
    e.manager_id AS employee_manager_id,
    m.salary AS manager_salary,
    m.email AS manager_email,
    m.dept_id AS manager_dept_id
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
