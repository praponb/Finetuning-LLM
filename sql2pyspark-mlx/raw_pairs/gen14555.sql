SELECT
    e.id,
    e.salary AS employee_salary,
    m.dept_id AS manager_dept_id,
    m.level AS manager_level,
    m.salary AS manager_salary
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id
