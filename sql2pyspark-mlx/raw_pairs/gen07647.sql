SELECT id, budget, SUM(budget) OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_budget FROM departments WHERE budget > 2031000 ORDER BY running_budget DESC;
