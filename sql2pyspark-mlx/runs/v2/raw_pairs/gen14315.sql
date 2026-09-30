select
    id,
    location,
    dept_name,
    budget,
    row_number() over (partition by location, dept_name order by budget desc, id) as budget_row
from departments
where dept_name is not null;
