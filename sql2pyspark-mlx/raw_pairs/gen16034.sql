with valid_products as (
    select *
    from products
    where cost is not null
),
launch_year_summary as (
    select year(launch_date) as launch_year, min(launch_date) as min_launch_date
    from valid_products
    group by year(launch_date)
)
select *
from launch_year_summary
order by min_launch_date;
