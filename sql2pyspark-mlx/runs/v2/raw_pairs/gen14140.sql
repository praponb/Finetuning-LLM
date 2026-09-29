select
    region,
    city,
    date_format(opened_date, 'yyyy-MM') as opened_month,
    sort_array(collect_set(region)) as region_list,
    concat_ws(', ', sort_array(collect_set(region))) as region_values
from stores
group by region, city, date_format(opened_date, 'yyyy-MM')
order by city;
