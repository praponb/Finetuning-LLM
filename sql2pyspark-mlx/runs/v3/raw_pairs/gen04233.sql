SELECT
    id,
    level,
    hire_date,
    salary,
    SUM(salary) OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
FROM employees
WHERE hire_date BETWEEN '2024-01-01' AND '2024-09-01';
