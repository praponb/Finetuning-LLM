select
    *,
    lag(budget, 2, 0) over (partition by location order by budget, id) as prev_budget,
    lead(budget, 1) over (order by budget, id) as next_budget,
    row_number() over (partition by dept_name order by budget desc, id) as row_seq
from departments
