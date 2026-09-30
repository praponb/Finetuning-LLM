SELECT
    id,
    full_name,
    email,
    hire_date,
    MAX(salary) OVER (PARTITION BY manager_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_salary,
    COUNT(salary) OVER (PARTITION BY manager_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_salary,
    MIN(salary) OVER (PARTITION BY manager_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_salary
FROM employees
WHERE full_name LIKE 'P%'
