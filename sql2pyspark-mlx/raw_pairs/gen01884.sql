with department_totals as (
    select dept_id, count(*) as cnt, round(avg(salary), 2) as avg_salary
    from employees
    group by dept_id
)
select
    d.id,
    d.dept_name,
    dt.cnt,
    dt.avg_salary
from departments d
inner join department_totals dt on d.id = dt.dept_id
where dt.avg_salary >= 72
order by d.id desc, cnt desc
