SELECT *
FROM (
    SELECT is_remote, level, AVG(salary) AS avg_salary
    FROM employees
    WHERE full_name LIKE '%Mia%'
    GROUP BY is_remote, level
) AS x
WHERE avg_salary >= 109000
ORDER BY level DESC
