SELECT
    id,
    full_name,
    email,
    dept_id,
    is_remote,
    level,
    MAX(salary) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_salary,
    SUM(salary) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_salary,
    MIN(salary) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_salary,
    AVG(salary) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_salary,
    LAG(salary, 2, 0) OVER (PARTITION BY job_title ORDER BY salary, id) AS prev_salary
FROM employees;
