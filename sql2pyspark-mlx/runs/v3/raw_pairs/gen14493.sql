with valid_stores as (
    select *, initcap(trim(region)) as region_title
    from stores
    where lower(store_name) like '%ive%'
),
city_summary as (
    select city, round(stddev(sqft), 2) as stddev_sqft, count(case when city = 'Tokyo' then 1 end) as tokyo_count
    from valid_stores
    group by city
)
select city, stddev_sqft, tokyo_count
from city_summary
