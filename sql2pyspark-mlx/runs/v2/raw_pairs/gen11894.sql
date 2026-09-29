select id, location
from departments
where dept_name in ('Engineering', 'Sales') and dept_name in ('Finance', 'HR');
