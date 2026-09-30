select
    id,
    email,
    salary,
    concat(full_name, '/', level) full_name_level
from employees
order by 3 desc, email desc, id
limit 3;
