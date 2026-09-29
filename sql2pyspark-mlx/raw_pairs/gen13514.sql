select id, dept_name, location, budget, round(budget / sum(budget) over (partition by dept_name, location), 3) budget_share from departments where location = 'Berlin' order by budget, location;
