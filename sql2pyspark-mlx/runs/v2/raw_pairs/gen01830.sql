SELECT
    id,
    manager_id,
    hire_date,
    salary,
    ROUND(AVG(salary) OVER (PARTITION BY manager_id ORDER BY hire_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS salary_moving_avg_7
FROM employees;
