SELECT
    *,
    AVG(salary) OVER (ORDER BY hire_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS salary_avg_5_prior,
    NTILE(3) OVER (ORDER BY salary DESC, id) AS salary_bucket,
    MAX(salary) OVER (PARTITION BY dept_id) AS dept_id_max_salary
FROM employees;
