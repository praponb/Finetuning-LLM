select
    d.location,
    d.budget,
    d.dept_name,
    e.dept_id
from departments d
left join employees e on d.id = e.dept_id
order by d.budget desc, dept_name desc;
