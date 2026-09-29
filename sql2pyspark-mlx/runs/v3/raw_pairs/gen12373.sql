SELECT *
FROM (
    SELECT level, MAX(salary) - MIN(salary) AS salary_range, ROUND(AVG(salary), 2) AS avg_salary
    FROM employees
    GROUP BY level
) AS x
WHERE avg_salary > 72
ORDER BY level DESC, avg_salary;
