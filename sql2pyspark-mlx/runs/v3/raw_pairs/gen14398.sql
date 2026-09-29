select
    d.dept_name,
    d.budget,
    d.location,
    e.salary,
    e.hire_date
from departments d
left outer join employees e on d.id = e.dept_id;
