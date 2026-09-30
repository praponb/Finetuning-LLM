select id, location, budget, sum(budget) over (partition by location) as location_sum_budget from departments where budget >= 3214000 order by location_sum_budget, location desc
