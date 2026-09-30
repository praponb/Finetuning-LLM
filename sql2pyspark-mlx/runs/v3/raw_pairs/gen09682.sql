select
    region,
    sum(case when city = 'London' then sqft else 0 end) as london_sqft,
    concat_ws(', ', sort_array(collect_set(city))) as city_values,
    round((100 * sum(case when region = 'East' then 1 else 0 end)) / count(*), 1) as pct_east
from stores
group by region
