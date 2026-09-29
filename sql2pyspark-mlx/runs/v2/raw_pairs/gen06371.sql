with department_stats as (
    select dept_id, min(hire_date) as first_hire_date, round((100 * sum(case when job_title = 'Intern' then 1 else 0 end)) / count(*), 1) as pct_intern
    from employees
    group by dept_id
)
select
    d.id,
    d.budget,
    ds.first_hire_date,
    ds.pct_intern
from departments d
left join department_stats ds on d.id = ds.dept_id
where ds.pct_intern > 34
order by ds.first_hire_date
