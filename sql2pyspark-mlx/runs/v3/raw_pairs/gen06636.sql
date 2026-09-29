select
    id,
    location,
    dept_name,
    budget,
    lag(budget, 2) over (partition by location, dept_name order by budget, id) as prev_budget
from departments;
