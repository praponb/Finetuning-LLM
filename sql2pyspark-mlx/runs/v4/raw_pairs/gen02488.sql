SELECT id, budget, LEAD(budget, 1) OVER (ORDER BY budget, id) AS next_budget FROM departments;
