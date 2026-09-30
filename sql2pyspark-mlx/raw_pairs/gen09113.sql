select *, min(budget) over (partition by location, dept_name) as location_min_budget, lead(budget, 1) over (partition by dept_name order by budget, id) as next_budget from departments
