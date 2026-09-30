select
    id,
    dept_name,
    location,
    budget,
    avg(budget) over (order by budget, id rows between 2 preceding and 1 preceding) as budget_avg_2_prior,
    max(budget) over (partition by dept_name) as dept_name_max_budget
from departments
where dept_name in ('Engineering', 'HR', 'Marketing');
