SELECT id, budget, PERCENT_RANK() OVER (ORDER BY budget) AS budget_pct_rank
FROM departments
ORDER BY 1;
