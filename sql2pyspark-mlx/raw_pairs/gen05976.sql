SELECT *, ROUND(budget * 1.15, 2) AS budget_adjusted FROM departments WHERE location <> 'Singapore' AND location IN ('London', 'New York', 'Berlin') AND budget > 3025000 ORDER BY budget_adjusted DESC
