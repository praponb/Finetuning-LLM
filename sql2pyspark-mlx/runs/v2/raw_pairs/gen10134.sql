SELECT id, budget, LAG(budget, 1, 0) OVER (ORDER BY budget, id) prev_budget FROM departments WHERE location IS NULL
