select
    id,
    location,
    budget,
    round(avg(budget) over (partition by location order by budget, id rows between 6 preceding and current row), 2) as budget_moving_avg_7,
    round(budget / sum(budget) over (partition by location), 3) as budget_share
from departments
order by budget_share desc, budget desc;
