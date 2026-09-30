select
    e.id,
    e.level,
    e.salary,
    e.email
from employees e
where e.dept_id in (select id from departments where budget between 2396000 and 3273000);
