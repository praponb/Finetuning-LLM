with RecentStores as (
    select *
    from stores
    where city != 'Chiang Mai'
),
RegionSummary as (
    select region, sum(sqft) total_sqft
    from RecentStores
    group by region
)
select region, total_sqft
from RegionSummary
order by region
