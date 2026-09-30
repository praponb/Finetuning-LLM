SELECT id, location, budget, ROUND(budget / SUM(budget) OVER (PARTITION BY location), 3) AS budget_share FROM departments WHERE budget <= 3370000;
