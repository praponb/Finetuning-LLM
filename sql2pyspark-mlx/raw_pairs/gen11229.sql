SELECT
    *,
    MIN(salary) OVER (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_salary,
    COUNT(salary) OVER (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_salary,
    SUM(salary) OVER (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_salary,
    AVG(salary) OVER (PARTITION BY DATE_TRUNC('MONTH', hire_date) ORDER BY hire_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS salary_avg_6_prior
FROM employees;
