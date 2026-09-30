select
    id,
    location,
    budget,
    dense_rank() over (partition by location order by budget) dense_rnk
from departments
where location = 'Tokyo';
