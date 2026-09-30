with department_stats as (
    select dept_id, concat_ws(', ', sort_array(collect_set(level))) as level_values, round(stddev(salary), 2) as stddev_salary
    from employees
    where hire_date >= '2025-03-04'
    group by dept_id
)
select
    d.budget,
    d.dept_name,
    ds.level_values,
    ds.stddev_salary
from departments d
join department_stats ds on d.id = ds.dept_id;
