select
    region,
    date_trunc('MONTH', opened_date) as opened_month,
    count(distinct sqft) as unique_regions,
    count(distinct sqft) as unique_citys,
    concat_ws(', ', sort_array(collect_set(region))) as region_values
from stores
group by region, date_trunc('MONTH', opened_date)
order by opened_month
