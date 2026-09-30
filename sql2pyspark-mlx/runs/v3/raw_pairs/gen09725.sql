SELECT id, location, budget, AVG(budget) OVER (PARTITION BY location) AS location_avg_budget, RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS budget_rank FROM departments;
