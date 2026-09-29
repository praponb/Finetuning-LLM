SELECT id, budget FROM departments WHERE budget > (SELECT AVG(budget) FROM departments WHERE budget BETWEEN 2411000 AND 3115000)
