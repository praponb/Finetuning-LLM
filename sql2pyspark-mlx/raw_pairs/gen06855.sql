select
    id,
    budget,
    dept_name,
    rank() over (order by budget desc) as rnk,
    lag(budget, 1) over (partition by dept_name order by budget, id) as prev_budget
from departments
where dept_name in ('Sales', 'HR')
