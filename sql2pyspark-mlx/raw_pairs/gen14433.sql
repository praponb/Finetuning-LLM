with department_stats as (
    select dept_id, round((100 * sum(case when level = 'L2' then 1 else 0 end)) / count(*), 1) pct_l2, round(stddev(salary), 2) stddev_salary
    from employees
    group by dept_id
)
select
    d.id,
    d.budget,
    d.dept_name,
    ds.pct_l2,
    ds.stddev_salary
from departments d
left join department_stats ds on d.id = ds.dept_id
where ds.stddev_salary >= 72
order by budget desc;
