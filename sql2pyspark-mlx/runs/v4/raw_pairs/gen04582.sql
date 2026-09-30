select
    region,
    city,
    round((100 * sum(case when region = 'West' then 1 else 0 end)) / count(*), 1) as pct_west,
    concat_ws(', ', sort_array(collect_set(region))) as region_values,
    max(sqft) as max_sqft
from stores
group by region, city;
