SELECT
    id,
    hire_date,
    salary,
    ROUND(AVG(salary) OVER (ORDER BY hire_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS salary_moving_avg_3
FROM employees
ORDER BY hire_date DESC, salary
