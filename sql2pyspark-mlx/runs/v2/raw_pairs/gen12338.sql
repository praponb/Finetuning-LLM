select id, location, budget, max(budget) over (partition by location) as location_max_budget from departments where dept_name is null
