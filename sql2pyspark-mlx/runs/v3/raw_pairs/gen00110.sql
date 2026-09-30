with department_stats as (
    select dept_id, avg(salary) as avg_salary
    from employees
    where dept_id in (18, 23, 40)
    group by dept_id
)
select d.id, d.dept_name, ds.avg_salary
from departments d
inner join department_stats ds on d.id = ds.dept_id;
