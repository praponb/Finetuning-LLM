select d.location, count(e.id) as employee_count
from departments d
left join employees e on d.id = e.dept_id
group by d.location
order by employee_count desc, d.location
