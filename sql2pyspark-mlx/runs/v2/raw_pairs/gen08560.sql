select
    id,
    location,
    dept_name,
    budget,
    dense_rank() over (partition by location, dept_name order by budget) as dense_rnk
from departments;
