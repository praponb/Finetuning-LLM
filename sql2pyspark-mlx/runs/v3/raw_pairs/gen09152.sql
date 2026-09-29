select
    e.id,
    e.level,
    e.hire_date,
    e.email
from employees e
where e.dept_id in (select id from departments where budget > 2210000)
order by e.hire_date, id desc;
