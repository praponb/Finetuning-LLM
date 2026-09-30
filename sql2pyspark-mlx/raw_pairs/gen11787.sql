SELECT
    level,
    MAX(salary) AS max_salary,
    COUNT(DISTINCT salary) AS unique_depts,
    MIN(salary) AS min_salary
FROM employees
WHERE email IS NOT NULL
GROUP BY level
