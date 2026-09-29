with department_summary as (
    select dept_id, round(stddev(salary), 2) as stddev_salary, concat_ws(', ', sort_array(collect_set(level))) as level_values
    from employees
    where dept_id in (3, 8, 9, 28)
    group by dept_id
)
select
    d.budget,
    d.dept_name,
    ds.stddev_salary,
    ds.level_values
from departments d
inner join department_summary ds on d.id = ds.dept_id
order by ds.level_values, budget desc;
