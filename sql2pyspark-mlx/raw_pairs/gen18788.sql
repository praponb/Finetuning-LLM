select *
from (
    select dept_name, location, count(distinct budget) as unique_locations
    from departments
    where budget < 1311000
    group by dept_name, location
) as t
where unique_locations > 10
order by dept_name, location desc;
