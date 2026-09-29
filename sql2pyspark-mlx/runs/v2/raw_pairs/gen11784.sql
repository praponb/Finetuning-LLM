with department_summary as (
    select dept_id, max(salary) as max_salary
    from employees
    where full_name like '%Kim%'
    group by dept_id
)
select
    d.id,
    d.location,
    d.budget,
    ds.max_salary
from departments d
join department_summary ds on d.id = ds.dept_id
where ds.max_salary > 95000;
