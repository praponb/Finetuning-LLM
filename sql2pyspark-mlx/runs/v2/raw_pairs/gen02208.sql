SELECT SUM(budget) AS total_budget, COUNT(budget) AS budget_count FROM departments WHERE budget BETWEEN 2925000 AND 3763000 ORDER BY budget_count DESC;
