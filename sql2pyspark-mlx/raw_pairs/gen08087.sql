SELECT
    id,
    dept_id,
    job_title,
    salary,
    is_remote,
    level,
    MAX(salary) OVER w_grp AS running_max_salary,
    SUM(salary) OVER w_grp AS running_sum_salary,
    AVG(salary) OVER w_grp AS running_avg_salary,
    PERCENT_RANK() OVER w_hist AS salary_percent_rank
FROM employees
WINDOW w_grp AS (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY manager_id, job_title ORDER BY salary);
