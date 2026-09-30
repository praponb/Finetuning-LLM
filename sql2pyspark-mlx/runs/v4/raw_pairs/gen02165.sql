SELECT
    *,
    LAG(salary, 1, 0) OVER (PARTITION BY job_title ORDER BY hire_date, id) AS prev_salary,
    LAG(salary, 2) OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS prev_salary_2,
    LAST_VALUE(level, TRUE) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level
FROM employees
