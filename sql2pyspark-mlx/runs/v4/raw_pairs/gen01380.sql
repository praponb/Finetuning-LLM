SELECT
    id,
    level,
    dept_id,
    ADD_MONTHS(hire_date, 12) AS hire_date_plus_months,
    SPLIT(LOWER(email), '@')[0] AS email_user
FROM employees
WHERE manager_id IS NULL AND (level IN ('L5', 'L3', 'L2') OR YEAR(hire_date) = 2024);
