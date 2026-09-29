select id, budget, dept_name, location, lead(budget, 1) over (order by budget, id) next_budget, lag(budget, 1) over (partition by dept_name, location order by budget, id) prev_budget from departments
