SELECT
    *,
    AVG(budget) OVER (ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS budget_avg_2,
    NTILE(4) OVER (ORDER BY budget DESC, id) AS budget_bucket,
    SUM(budget) OVER w_part AS running_budget,
    AVG(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS budget_avg_5_prior
FROM departments
WINDOW w_part AS (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
