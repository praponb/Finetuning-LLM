SELECT
    is_remote,
    dept_id,
    full_name,
    TRIM(email) AS clean_email,
    UPPER(full_name) AS full_name_upper
FROM employees
WHERE email IS NOT NULL AND dept_id IN (10, 21) AND hire_date BETWEEN '2024-06-01' AND '2025-09-01'
ORDER BY clean_email;
