select
    departments.location,
    departments.dept_name,
    employees.dept_id,
    employees.email,
    employees.full_name
from departments
left outer join employees on employees.dept_id = departments.id
where departments.location = 'Berlin'
order by location
