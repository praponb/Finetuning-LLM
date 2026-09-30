select e.id, e.manager_id, e.dept_id
from employees e
where e.dept_id in (select id from departments where location not in ('Chiang Mai', 'Singapore', 'London'));
