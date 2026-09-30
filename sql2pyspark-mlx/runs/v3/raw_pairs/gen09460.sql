with employees_by_department as (
    select dept_id, round((100 * sum(case when level = 'L2' then 1 else 0 end)) / count(*), 1) as pct_l2, concat_ws(', ', sort_array(collect_set(job_title))) as job_title_values
    from employees
    where level is not null
    group by dept_id
)
select
    d.id,
    d.budget,
    ebd.pct_l2,
    ebd.job_title_values
from departments d
inner join employees_by_department ebd on d.id = ebd.dept_id
order by d.id desc
limit 50;
