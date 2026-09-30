SELECT dept_name, SUM(budget) AS total_budget, DENSE_RANK() OVER (ORDER BY SUM(budget) DESC) AS budget_rank FROM departments GROUP BY dept_name ORDER BY total_budget DESC, dept_name;
