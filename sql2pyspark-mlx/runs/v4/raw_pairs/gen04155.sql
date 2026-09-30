with employees_by_department as (
    select dept_id, round((100 * sum(case when job_title = 'Designer' then 1 else 0 end)) / count(*), 1) pct_designer
    from employees
    group by dept_id
)
select d.id, d.budget, ebd.pct_designer
from departments d
left join employees_by_department ebd on d.id = ebd.dept_id
order by d.id;
