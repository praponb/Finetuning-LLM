with FilteredStores as (
    select *
    from stores
    where sqft > 15444 and region = 'East'
),
region_summary as (
    select region, sum(case when city = 'New York' then 1 else 0 end) as new_york_count, count(sqft) as sqft_count
    from FilteredStores
    group by region
)
select *
from region_summary
order by sqft_count desc;
