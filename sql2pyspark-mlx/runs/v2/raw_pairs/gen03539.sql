SELECT
    manager_id,
    level,
    COUNT(DISTINCT salary) unique_depts,
    MIN(salary) min_salary,
    ROUND(AVG(salary), 2) avg_salary
FROM employees
WHERE email LIKE '%@gmail.com' AND salary <= 91000
GROUP BY manager_id, level
HAVING COUNT(DISTINCT salary) < 5;
