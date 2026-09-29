SELECT id, budget, dept_name, CEIL(budget) budget_whole FROM departments WHERE dept_name NOT IN ('Finance', 'HR', 'Sales') ORDER BY budget_whole DESC
