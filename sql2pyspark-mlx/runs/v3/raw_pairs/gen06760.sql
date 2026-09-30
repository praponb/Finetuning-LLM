select
    e.id,
    e.level as employee_level,
    m.manager_id as manager_manager_id,
    m.is_remote as manager_is_remote,
    m.dept_id as manager_dept_id
from employees e
inner join employees m on e.manager_id = m.id
where m.is_remote and m.manager_id is null
order by e.level desc
