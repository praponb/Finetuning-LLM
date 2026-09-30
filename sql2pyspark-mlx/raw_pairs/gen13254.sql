SELECT
    email,
    hire_date,
    manager_id,
    TRIM(email) clean_email
FROM employees;
