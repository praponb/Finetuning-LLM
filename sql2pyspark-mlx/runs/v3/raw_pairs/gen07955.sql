SELECT id, budget FROM departments WHERE budget > (SELECT AVG(budget) FROM departments) ORDER BY budget ASC;
