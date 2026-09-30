SELECT MAX(budget) - MIN(budget) AS budget_range, MAX(budget) AS max_budget, ROUND(STDDEV(budget), 2) AS stddev_budget FROM departments WHERE location = 'Singapore' ORDER BY budget_range DESC
