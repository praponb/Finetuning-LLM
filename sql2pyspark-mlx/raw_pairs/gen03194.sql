SELECT id, budget, PERCENT_RANK() OVER (ORDER BY budget DESC) AS budget_pct_rank FROM departments;
