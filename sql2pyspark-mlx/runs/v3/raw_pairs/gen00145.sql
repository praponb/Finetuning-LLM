select departments.id, departments.location, departments.dept_name
from departments
where departments.location in ('Singapore', 'New York', 'Berlin') and not exists (select 1 from employees where employees.dept_id = departments.id);
