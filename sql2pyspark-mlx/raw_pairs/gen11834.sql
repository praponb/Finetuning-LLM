select d.dept_name, e.level
from departments d
left join employees e on d.id = e.dept_id;
