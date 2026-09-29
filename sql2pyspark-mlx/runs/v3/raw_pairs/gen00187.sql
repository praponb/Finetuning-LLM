select
    employees.id,
    employees.full_name,
    employees.job_title,
    employees.email,
    departments.dept_name,
    departments.budget
from employees
left join departments on employees.dept_id = departments.id
where departments.location = 'Berlin'
order by dept_name
