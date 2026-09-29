SELECT id, budget, location FROM departments WHERE budget = (SELECT MIN(budget) FROM departments) ORDER BY location DESC, budget DESC;
