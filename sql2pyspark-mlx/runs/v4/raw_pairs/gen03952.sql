select
    departments.id,
    departments.dept_name,
    departments.budget,
    departments.location
from departments
where departments.budget between 2333000 and 2845000 and exists (select 1 from employees where employees.dept_id = departments.id)
