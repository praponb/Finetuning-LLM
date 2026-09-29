select id, dept_name, budget, lead(budget, 2) over (partition by dept_name order by budget, id) as next_budget from departments order by next_budget desc
