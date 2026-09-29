select case when city in ('Tokyo', 'Berlin') then 'tier_1' else 'other' end as city_group, concat_ws(', ', sort_array(collect_set(city))) as city_values
from suppliers
where lower(trim(supplier_name)) = 'lion city imports' and lower(supplier_name) like '%lob%' and rating < 2.6
group by case when city in ('Tokyo', 'Berlin') then 'tier_1' else 'other' end
