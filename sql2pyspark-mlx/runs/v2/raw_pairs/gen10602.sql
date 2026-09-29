select
    id,
    dept_name,
    budget,
    round(avg(budget) over (partition by dept_name order by budget, id rows between 2 preceding and current row), 2) as budget_moving_avg_3
from departments
where budget <= 3066000
