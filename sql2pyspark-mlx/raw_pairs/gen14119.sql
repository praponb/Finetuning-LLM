select
    e.dept_id,
    e.salary,
    e.level,
    d.location,
    d.budget,
    d.dept_name,
    case when d.dept_name = 'Finance' then 1 else 0 end as is_finance
from employees e
left outer join departments d on e.dept_id = d.id;
