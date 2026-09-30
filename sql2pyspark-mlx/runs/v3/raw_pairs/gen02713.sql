select
    id,
    dept_name,
    budget,
    sum(budget) over (partition by dept_name order by budget, id) as running_budget
from departments
where location = 'Chiang Mai';
