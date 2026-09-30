with department_summary as (
    select dept_id, sort_array(collect_set(level)) as level_list
    from employees
    group by dept_id
)
select d.budget, d.location, ds.level_list
from departments d
inner join department_summary ds on d.id = ds.dept_id
order by location
