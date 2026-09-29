select id, dept_name, location
from departments
where location = 'Chiang Mai'
union
select id, dept_name, location
from departments
where dept_name = 'HR'
