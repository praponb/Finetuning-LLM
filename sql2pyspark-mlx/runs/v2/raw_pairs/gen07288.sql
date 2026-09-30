select e.id, e.is_remote, e.full_name
from employees e
where e.level in ('L1', 'L4', 'L2') and e.dept_id in (select id from departments where location in ('Singapore', 'Chiang Mai'))
order by id desc, full_name
