with ValidDepartments as (
    select *
    from departments
    where dept_name != 'HR' and dept_name not in ('Marketing', 'HR', 'Finance')
),
departments_stats as (
    select location, concat_ws(', ', sort_array(collect_set(dept_name))) as dept_name_values
    from ValidDepartments
    group by location
)
select *
from departments_stats
order by dept_name_values, location
