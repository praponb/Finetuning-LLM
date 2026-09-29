select dept_name
from departments
where budget > 2530000
union all
select dept_name
from departments
where location in ('London', 'Singapore', 'Berlin');
