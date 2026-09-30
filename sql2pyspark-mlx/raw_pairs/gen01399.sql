with department_stats as (
    select dept_id, max(hire_date) as last_hire_date, count(case when level = 'L4' then 1 end) as l4_count
    from employees
    group by dept_id
)
select
    d.id,
    d.location,
    d.dept_name,
    ds.last_hire_date,
    ds.l4_count
from departments d
inner join department_stats ds on d.id = ds.dept_id
