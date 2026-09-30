SELECT
    e.id,
    e.hire_date employee_hire_date,
    e.salary employee_salary,
    e.full_name employee_full_name,
    m.manager_id manager_manager_id
FROM employees AS e
LEFT JOIN employees AS m ON e.manager_id = m.id;
