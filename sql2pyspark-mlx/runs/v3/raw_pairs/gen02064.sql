with department_summary as (
    select dept_id, max(hire_date) as last_hire_date, min(hire_date) as min_hire_date
    from employees
    group by dept_id
)
select
    d.id,
    d.location,
    ds.last_hire_date,
    ds.min_hire_date
from departments d
inner join department_summary ds on d.id = ds.dept_id
