with valid_departments as (
    select *
    from departments
    where dept_name in ('HR', 'Marketing')
),
agg as (
    select location, concat_ws(', ', sort_array(collect_set(location))) as location_values
    from valid_departments
    group by location
)
select *
from agg
order by location_values, location
