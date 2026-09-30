with EmployeesByDepartment as (
    select dept_id, max(salary) - min(salary) as salary_range, min(hire_date) as min_hire_date
    from employees
    where level in ('L1', 'L3')
    group by dept_id
)
select
    d.id,
    d.location,
    d.dept_name,
    E.salary_range,
    E.min_hire_date
from departments d
inner join EmployeesByDepartment E on d.id = E.dept_id
order by d.location
