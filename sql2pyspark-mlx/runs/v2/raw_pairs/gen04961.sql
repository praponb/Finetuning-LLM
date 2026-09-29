SELECT dept_name, SUM(budget) total_budget, RANK() OVER (ORDER BY SUM(budget) DESC) AS budget_rank FROM departments WHERE location IS NOT NULL GROUP BY dept_name ORDER BY total_budget DESC
