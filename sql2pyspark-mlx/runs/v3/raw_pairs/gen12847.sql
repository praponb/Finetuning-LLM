SELECT
    dept_id,
    level,
    salary,
    CONCAT(full_name, '/', level) AS full_name_level
FROM employees
WHERE LOWER(TRIM(email)) = 'carol@company.co' AND manager_id IS NOT NULL
ORDER BY dept_id DESC
