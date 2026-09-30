select
    id,
    dept_name,
    location,
    budget,
    lead(budget, 2) over (partition by dept_name order by budget, id) as next_budget,
    first_value(dept_name, true) over (partition by location order by budget, id rows between unbounded preceding and current row) as first_known_dept_name
from departments
where dept_name = 'Marketing'
