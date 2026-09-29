SELECT is_remote, total_salary, avg_salary
FROM (
    SELECT is_remote, SUM(salary) AS total_salary, AVG(salary) AS avg_salary
    FROM employees
    WHERE full_name LIKE 'Hen%'
    GROUP BY is_remote
) AS agg
WHERE avg_salary >= 102000
ORDER BY 2;
