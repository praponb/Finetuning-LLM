select
    id,
    dept_name,
    budget,
    max(budget) over (partition by dept_name) dept_name_max_budget
from departments
where dept_name in ('Sales', 'Engineering');
