SELECT id, budget, location FROM departments WHERE budget = (SELECT MIN(budget) FROM departments WHERE budget >= 3855000) ORDER BY 3;
