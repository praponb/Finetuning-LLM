with department_summary as (
    select dept_id, count(*) as employee_count, count(distinct salary) as unique_managers
    from employees
    where level = 'L5'
    group by dept_id
)
select
    d.location,
    d.dept_name,
    ds.employee_count,
    ds.unique_managers
from departments d
inner join department_summary ds on d.id = ds.dept_id
order by unique_managers desc;
