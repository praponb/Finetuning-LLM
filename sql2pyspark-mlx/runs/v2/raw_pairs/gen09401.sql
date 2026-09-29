with department_summary as (
    select dept_id, count(*) as cnt, min(hire_date) as min_hire_date
    from employees
    group by dept_id
)
select
    d.id,
    d.dept_name,
    ds.cnt,
    ds.min_hire_date
from departments d
join department_summary ds on d.id = ds.dept_id
where ds.cnt > 5
