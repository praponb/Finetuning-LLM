SELECT dept_name, ROUND(budget * 1.15, 2) AS budget_adjusted
FROM departments
WHERE budget <= 3893000 AND (budget < 3005000 OR dept_name NOT IN ('HR', 'Marketing'));
