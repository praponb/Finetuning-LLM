select
    id,
    location,
    budget,
    row_number() over (partition by location order by budget, id) as row_num
from departments
order by id;
