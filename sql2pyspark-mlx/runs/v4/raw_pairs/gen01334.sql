select
    id,
    dept_name,
    location,
    budget,
    lower(dept_name) as dept_name_lower,
    cast(budget as int) as budget_int
from departments
where budget <= 3066000 and dept_name = 'HR';
