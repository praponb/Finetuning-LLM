SELECT is_remote, row_count, avg_salary
FROM (
    SELECT is_remote, COUNT(*) AS row_count, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY is_remote
) AS s
WHERE avg_salary > 88000
ORDER BY row_count DESC
