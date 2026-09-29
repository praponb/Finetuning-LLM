select city, sort_array(collect_set(city)) as city_list, round(stddev(sqft), 2) as stddev_sqft
from stores
group by city
order by city, stddev_sqft desc
limit 20;
