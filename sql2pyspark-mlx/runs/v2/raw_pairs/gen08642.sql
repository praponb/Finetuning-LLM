select id, location, budget, sum(budget) over (partition by location order by budget, id) as running_budget from departments where dept_name = 'Engineering';
