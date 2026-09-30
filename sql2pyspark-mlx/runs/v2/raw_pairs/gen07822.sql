with recent_stores as (
    select *
    from stores
    where lower(trim(store_name)) = 'airport kiosk' and sqft != 15141 and opened_date < '2024-04-26'
),
agg as (
    select city, round(stddev(sqft), 2) as stddev_sqft
    from recent_stores
    group by city
)
select *
from agg
where stddev_sqft < 63
order by city asc;
