select
    d.dept_name,
    d.budget,
    d.location,
    e.level,
    e.dept_id,
    e.hire_date
from departments d
inner join employees e on e.dept_id = d.id
where d.budget >= 1162000 and e.level = 'L4'
order by dept_name
