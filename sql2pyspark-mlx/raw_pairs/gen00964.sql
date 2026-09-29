SELECT id, budget, SUM(budget) OVER (ORDER BY budget, id) AS running_budget
FROM departments
ORDER BY running_budget DESC;
