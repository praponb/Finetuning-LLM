SELECT id, budget, location FROM departments WHERE budget > (SELECT MAX(budget) FROM departments WHERE budget > 1444000) ORDER BY location, id LIMIT 10;
