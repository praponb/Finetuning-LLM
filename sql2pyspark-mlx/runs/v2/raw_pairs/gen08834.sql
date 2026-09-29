select
    d.dept_name,
    d.budget,
    e.is_remote,
    e.email,
    e.hire_date
from departments d
inner join employees e on d.id = e.dept_id;
