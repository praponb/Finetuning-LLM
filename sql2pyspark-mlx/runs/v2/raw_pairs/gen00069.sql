SELECT id, budget, ROUND(budget, 0) AS budget_rounded FROM departments WHERE location IS NULL ORDER BY budget DESC, id LIMIT 5;
