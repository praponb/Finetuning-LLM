select
    id,
    dept_name,
    budget,
    max(budget) over (partition by dept_name) as dept_name_max_budget
from departments
order by budget desc
