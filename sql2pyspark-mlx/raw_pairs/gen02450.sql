select
    region,
    city,
    size(collect_set(city)) as num_cities,
    sort_array(collect_set(region)) as region_list,
    sum(sqft) as total_sqft,
    stddev_samp(sqft) as sqft_stddev,
    count(distinct city) as distinct_cities
from stores
group by region, city
order by distinct_cities;
