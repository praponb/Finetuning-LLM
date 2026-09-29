SELECT id, budget FROM departments WHERE budget >= (SELECT MAX(budget) FROM departments);
