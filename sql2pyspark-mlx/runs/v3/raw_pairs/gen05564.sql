SELECT
    id,
    salary,
    email,
    level,
    CONCAT(email, ' ', full_name) AS email_full_name
FROM employees
WHERE level IN ('L2', 'L3') AND dept_id = 20
ORDER BY level;
