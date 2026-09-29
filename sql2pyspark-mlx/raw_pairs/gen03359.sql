with department_summary as (
    select dept_id, count(case when level = 'L2' then 1 end) l2_count, count(case when job_title = 'Intern' then 1 end) intern_count
    from employees
    where is_remote
    group by dept_id
)
select
    d.id,
    d.location,
    d.budget,
    ds.l2_count,
    ds.intern_count
from departments d
left join department_summary ds on d.id = ds.dept_id
where ds.intern_count > 6
order by budget, ds.intern_count desc;
