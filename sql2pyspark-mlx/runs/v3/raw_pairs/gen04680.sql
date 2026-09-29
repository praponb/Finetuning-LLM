SELECT
    e.salary AS employee_salary,
    e.is_remote AS employee_is_remote,
    e.dept_id AS employee_dept_id,
    m.salary AS manager_salary,
    m.hire_date AS manager_hire_date,
    DATE_FORMAT(e.hire_date, 'MMM yyyy') AS hire_date_label
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE e.level = 'L2';
