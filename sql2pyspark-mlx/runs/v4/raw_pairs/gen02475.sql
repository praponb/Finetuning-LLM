with base as (
    select *
    from products
    where category = 'Fashion'
),
Agg as (
    select brand, min(launch_date) min_launch_date, max(launch_date) latest_launch_date
    from base
    group by brand
)
select brand, min_launch_date
from Agg
order by brand desc, min_launch_date;
