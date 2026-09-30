select count(age) as age_count, concat_ws(', ', sort_array(collect_set(city))) as city_values, count(*) as total_count
from customers
where country = 'JP' and email not like '%@startup.io'
order by total_count
