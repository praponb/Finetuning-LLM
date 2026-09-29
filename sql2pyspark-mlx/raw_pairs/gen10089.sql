select
    id,
    dept_name,
    budget,
    ntile(3) over (partition by dept_name order by budget, id) as quartile
from departments
order by dept_name;
