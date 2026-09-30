with department_stats as (
    select dept_id, count(distinct salary) as unique_depts, round(stddev(salary), 2) as stddev_salary
    from employees
    group by dept_id
)
select
    d.id,
    d.location,
    d.budget,
    ds.unique_depts,
    ds.stddev_salary
from departments d
join department_stats ds on d.id = ds.dept_id
where ds.unique_depts >= 6
order by stddev_salary desc, id
limit 10
