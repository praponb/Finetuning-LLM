select
    e.id,
    e.is_remote,
    e.level,
    d.budget,
    lower(e.level) as level_lower
from employees e
inner join departments d on e.dept_id = d.id
where d.dept_name = 'Engineering' and d.dept_name = 'Finance'
order by level_lower desc, budget
