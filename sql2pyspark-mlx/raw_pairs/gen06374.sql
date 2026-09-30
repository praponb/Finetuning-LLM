SELECT
    level,
    COUNT(DISTINCT salary) unique_depts,
    MIN(salary) min_salary,
    MAX(salary) - MIN(salary) salary_range
FROM employees
WHERE dept_id IN (17, 21, 34)
GROUP BY level
ORDER BY level DESC, salary_range
