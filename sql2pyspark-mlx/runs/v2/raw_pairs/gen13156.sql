SELECT
    id,
    manager_id,
    hire_date,
    salary,
    SUM(salary) OVER (PARTITION BY manager_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
FROM employees
ORDER BY 5;
