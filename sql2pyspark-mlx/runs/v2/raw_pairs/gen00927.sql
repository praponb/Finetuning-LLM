with department_totals as (
    select dept_id, concat_ws(', ', sort_array(collect_set(level))) as level_values, max(salary) as max_salary
    from employees
    where level = 'L2'
    group by dept_id
)
select
    d.id,
    d.dept_name,
    dt.level_values,
    dt.max_salary
from departments d
join department_totals dt on d.id = dt.dept_id
where dt.max_salary > 116000
