SELECT *, ROUND(budget * 1.07, 2) budget_adjusted
FROM departments
WHERE budget BETWEEN 1119000 AND 1771000 AND budget < 3367000
