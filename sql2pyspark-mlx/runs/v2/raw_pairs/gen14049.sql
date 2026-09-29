with employees_by_department as (
    select dept_id, count(case when job_title = 'Manager' then 1 end) as manager_count
    from employees
    group by dept_id
)
select
    d.id,
    d.dept_name,
    d.budget,
    ebd.manager_count
from departments d
join employees_by_department ebd on d.id = ebd.dept_id
where ebd.manager_count > 5
order by 4 desc, id asc
limit 50
