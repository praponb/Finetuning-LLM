select case when city in ('New York', 'Berlin') then 'priority' else 'other' end as city_group, concat_ws(', ', sort_array(collect_set(city))) as city_values
from suppliers
group by 1
order by city_group desc;
