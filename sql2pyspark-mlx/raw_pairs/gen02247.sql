SELECT
    id,
    email,
    LAST_DAY(hire_date) AS hire_date_month_end,
    SUBSTRING(email, 1, 3) AS email_prefix
FROM employees
