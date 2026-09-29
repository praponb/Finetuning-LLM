select
    id,
    dept_name,
    budget,
    round(budget / sum(budget) over (partition by dept_name), 3) as budget_share,
    round(avg(budget) over (order by budget, id rows between 3 preceding and current row), 2) as budget_moving_avg_4
from departments
