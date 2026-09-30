SELECT
    *,
    SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget,
    LAST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name,
    NTILE(4) OVER (PARTITION BY location ORDER BY budget, id) AS budget_bucket
FROM departments
