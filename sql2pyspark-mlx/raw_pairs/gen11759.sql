with department_stats as (
    select dept_id, sort_array(collect_set(level)) as level_list
    from employees
    where manager_id is not null
    group by dept_id
)
select d.id, d.dept_name, ds.level_list
from departments d
left outer join department_stats ds on d.id = ds.dept_id
order by dept_name;
