select
    id,
    dept_name,
    location,
    budget,
    round(avg(budget) over (partition by dept_name, location order by budget, id rows between 6 preceding and current row), 2) budget_moving_avg_7
from departments
order by budget_moving_avg_7 desc
