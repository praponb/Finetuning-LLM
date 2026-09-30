select
    employees.id,
    employees.email,
    departments.dept_name,
    departments.location
from employees
inner join departments on employees.dept_id = departments.id
where employees.is_remote;
