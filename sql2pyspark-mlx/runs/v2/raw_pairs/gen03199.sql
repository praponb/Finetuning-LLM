select
    id,
    dept_name,
    budget,
    lead(budget, 1) over (partition by dept_name order by budget, id) as next_budget
from departments
where budget is null
