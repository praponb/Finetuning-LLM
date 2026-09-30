SELECT dept_name, COALESCE(budget, 0) budget_filled
FROM departments;
