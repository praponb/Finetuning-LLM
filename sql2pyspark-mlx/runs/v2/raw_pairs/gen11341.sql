select is_active, city_values, singapore_count
from (
    select is_active, concat_ws(', ', sort_array(collect_set(city))) as city_values, count(case when city = 'Singapore' then 1 end) as singapore_count
    from customers
    where email like '%@gmail.com'
    group by is_active
) as s
where singapore_count >= 8
