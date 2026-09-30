with department_totals as (
    select dept_id, count(case when level = 'L3' then 1 end) as l3_count, count(case when job_title = 'Intern' then 1 end) as intern_count
    from employees
    group by dept_id
)
select
    d.budget,
    d.location,
    dt.l3_count,
    dt.intern_count
from departments d
left outer join department_totals dt on d.id = dt.dept_id
