select
    id,
    budget,
    location,
    budget - lag(budget, 1) over (order by budget, id) as budget_change,
    sum(budget) over (partition by location order by budget, id) as running_budget
from departments
