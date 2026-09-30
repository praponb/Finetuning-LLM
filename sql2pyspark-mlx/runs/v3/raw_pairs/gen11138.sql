select
    id,
    dept_name,
    budget,
    round(budget / sum(budget) over (partition by dept_name), 3) as budget_share
from departments
order by budget_share desc
