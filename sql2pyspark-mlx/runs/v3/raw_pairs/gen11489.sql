SELECT budget, location, dept_name, ROUND(budget * 0.85, 2) AS budget_adjusted FROM departments WHERE budget >= 2990000;
