SELECT
    e.id,
    e.email AS employee_email,
    m.manager_id AS manager_manager_id,
    m.is_remote AS manager_is_remote,
    ADD_MONTHS(e.hire_date, 6) AS hire_date_plus_months
FROM employees AS e
LEFT JOIN employees AS m ON e.manager_id = m.id;
