with cleaned as (
    select
        *,
        trim(location) as clean_location,
        coalesce(dept_name, 'NONE') as resolved_dept_name
    from departments
    where location is not null
      and budget >= 3896000
)
select id, location
from cleaned
intersect
select id, location
from departments
where dept_name in ('Sales', 'Legal', 'Finance');
