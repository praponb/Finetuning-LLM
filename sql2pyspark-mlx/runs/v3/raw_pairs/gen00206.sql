select
    city,
    quarter(opened_date) opened_quarter,
    sort_array(collect_set(region)) region_list,
    count(distinct sqft) unique_citys
from stores
where region = 'East' and sqft > 14550 and store_name like 'Air%'
group by city, opened_quarter
having sum(case when region = 'North' then 1 else 0 end) >= 48
order by 4
