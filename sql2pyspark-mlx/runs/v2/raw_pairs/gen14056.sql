select id, location
from departments
where budget >= 1926000
union all
select id, location
from departments
where dept_name in ('Engineering', 'Sales')
