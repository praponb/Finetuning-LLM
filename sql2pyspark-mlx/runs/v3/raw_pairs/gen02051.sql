select id, budget, location, round(budget * 1.15, 2) as budget_adjusted from departments where dept_name in ('Engineering', 'Legal');
