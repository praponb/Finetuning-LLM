SELECT
    salary,
    hire_date,
    full_name,
    SPLIT(LOWER(email), '@')[0] AS email_user
FROM employees
ORDER BY email_user, hire_date;
