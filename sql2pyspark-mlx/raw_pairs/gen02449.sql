with filtered_stores as (
    select *
    from stores
    where sqft = 14440 and store_name not like '%Airport%'
),
summary as (
    select region, count(*) as total_count, round(stddev(sqft), 2) as stddev_sqft
    from filtered_stores
    group by region
)
select region, total_count, stddev_sqft
from summary
where stddev_sqft < 38
order by 2 desc;
