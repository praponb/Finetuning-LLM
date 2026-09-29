select d.budget, d.location, e.full_name
from departments d
left outer join employees e on e.dept_id = d.id
