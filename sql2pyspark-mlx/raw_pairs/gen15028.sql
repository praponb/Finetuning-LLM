SELECT
    *,
    LAST_VALUE(budget, TRUE) OVER w_running AS last_known_budget,
    NTILE(5) OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS budget_bucket,
    ROW_NUMBER() OVER (ORDER BY budget, id) AS event_seq,
    PERCENT_RANK() OVER w_hist AS budget_percent_rank,
    FIRST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_dept_name
FROM departments
WINDOW w_running AS (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY budget);
