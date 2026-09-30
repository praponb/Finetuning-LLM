SELECT
    e.id,
    e.hire_date AS employee_hire_date,
    m.hire_date AS manager_hire_date,
    m.salary AS manager_salary,
    m.dept_id AS manager_dept_id
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE e.manager_id IS NULL
ORDER BY manager_dept_id ASC, employee_hire_date ASC;
