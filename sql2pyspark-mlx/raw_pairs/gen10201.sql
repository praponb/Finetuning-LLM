SELECT
    level,
    email,
    DATE_SUB(hire_date, 46) AS hire_date_minus_46d,
    COALESCE(email, 'N/A') AS email_filled
FROM employees
