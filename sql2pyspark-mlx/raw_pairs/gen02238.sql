SELECT
    id,
    manager_id,
    job_title,
    level,
    ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date ASC, id ASC) AS row_seq,
    FIRST_VALUE(level, TRUE) OVER (PARTITION BY DATE_TRUNC('YEAR', hire_date), manager_id ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level,
    ROW_NUMBER() OVER (ORDER BY hire_date DESC, id ASC) AS event_seq,
    CUME_DIST() OVER (PARTITION BY MONTH(hire_date), level ORDER BY salary ASC) AS salary_cume_dist
FROM employees
