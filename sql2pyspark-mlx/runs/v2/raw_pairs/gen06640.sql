SELECT
    id,
    manager_id,
    hire_date,
    PERCENT_RANK() OVER (PARTITION BY manager_id ORDER BY hire_date) AS hire_date_pct_rank
FROM employees
ORDER BY manager_id DESC;
