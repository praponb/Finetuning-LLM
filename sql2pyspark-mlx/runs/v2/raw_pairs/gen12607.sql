select
    d.budget,
    d.dept_name,
    d.location,
    e.hire_date
from departments d
left outer join employees e on d.id = e.dept_id
where d.location in ('London', 'Tokyo', 'Chiang Mai') and e.dept_id in (4, 28, 39)
