select id, dept_name, location
from departments
where dept_name = 'Sales'
except
select id, dept_name, location
from departments
where dept_name != 'Marketing';
