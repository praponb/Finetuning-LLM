select city, concat_ws(', ', sort_array(collect_set(region))) as region_values, concat_ws(', ', sort_array(collect_set(city))) as city_values
from stores
where (region in ('North', 'West', 'Central') and opened_date > '2025-09-01') or store_name like 'R%'
group by city
