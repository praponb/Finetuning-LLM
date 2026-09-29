SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id ASC) AS row_seq,
    ROUND(salary / SUM(salary) OVER (PARTITION BY MONTH(hire_date), level), 4) AS salary_share,
    LAST_VALUE(level) IGNORE NULLS OVER (PARTITION BY dept_id, manager_id ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
    DENSE_RANK() OVER w_grp AS salary_dense_rank,
    STDDEV_SAMP(salary) OVER (PARTITION BY DATE_TRUNC('MONTH', hire_date), level) AS hire_date_period_stddev_samp_salary
FROM employees
WHERE dept_id IN (22, 33)
WINDOW w_grp AS (PARTITION BY dept_id, manager_id ORDER BY salary ASC)
