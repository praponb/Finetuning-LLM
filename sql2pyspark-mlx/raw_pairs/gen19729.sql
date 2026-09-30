select
    id,
    location,
    budget,
    dense_rank() over (partition by location order by budget) as budget_dense_rank
from departments
