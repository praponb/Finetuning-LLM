SELECT
    id,
    level,
    hire_date,
    email,
    LAST_VALUE(email) OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_email
FROM employees;
