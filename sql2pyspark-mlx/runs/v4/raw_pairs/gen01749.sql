select
    d.dept_name,
    e.hire_date,
    e.dept_id,
    length(e.email) as email_length
from departments d
inner join employees e on e.dept_id = d.id;
