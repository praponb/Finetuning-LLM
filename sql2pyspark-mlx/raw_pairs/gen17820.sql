SELECT
    id,
    full_name,
    manager_id,
    LAST_DAY(hire_date) hire_date_month_end
FROM employees
WHERE level IN ('L3', 'L4', 'L2')
