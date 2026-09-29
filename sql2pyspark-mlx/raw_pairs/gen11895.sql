select id, location, budget, lead(budget, 2) over (partition by location order by budget, id) as next_budget from departments;
