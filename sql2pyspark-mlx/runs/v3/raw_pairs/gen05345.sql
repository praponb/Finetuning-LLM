SELECT
    id,
    dept_id,
    hire_date,
    PERCENT_RANK() OVER (PARTITION BY dept_id ORDER BY hire_date) hire_date_pct_rank
FROM employees
