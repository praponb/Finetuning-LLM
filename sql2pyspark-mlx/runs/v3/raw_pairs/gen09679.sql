SELECT id, budget
FROM departments
WHERE budget >= (SELECT AVG(budget) FROM departments WHERE budget > 1627000);
