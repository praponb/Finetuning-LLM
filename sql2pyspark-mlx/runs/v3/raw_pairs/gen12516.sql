select
    region,
    year(opened_date) as opened_year,
    concat_ws(', ', sort_array(collect_set(region))) as region_values,
    sum(case when region = 'West' then 1 else 0 end) as west_count,
    sort_array(collect_set(region)) as region_list
from stores
where opened_date <= '2023-05-01'
group by region, opened_year
