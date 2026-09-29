select departments.id, departments.location
from departments
where departments.budget < 2613000 and not exists (select 1 from employees where employees.dept_id = departments.id and employees.is_remote = true);
