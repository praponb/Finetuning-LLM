SELECT id, location, dept_name, budget, PERCENT_RANK() OVER (PARTITION BY location, dept_name ORDER BY budget) budget_pct_rank FROM departments;
