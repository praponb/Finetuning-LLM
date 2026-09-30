select id, dept_name, location
from departments
where budget is not null
order by dept_name, id
limit 1;
