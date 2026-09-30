SELECT id, budget, location
FROM departments
WHERE budget > (SELECT AVG(budget) FROM departments WHERE budget >= 2696000)
ORDER BY budget
