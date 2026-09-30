SELECT id, location, budget, RANK() OVER (PARTITION BY location ORDER BY budget) AS budget_rank FROM departments WHERE dept_name NOT IN ('HR', 'Finance', 'Legal');
