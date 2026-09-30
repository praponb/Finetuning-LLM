SELECT
    id,
    location,
    budget,
    PERCENT_RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS budget_pct_rank
FROM departments;
