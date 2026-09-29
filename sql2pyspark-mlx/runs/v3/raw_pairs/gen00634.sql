select id, dept_name, location
from departments
where location not in ('Singapore', 'New York')
order by id desc;
