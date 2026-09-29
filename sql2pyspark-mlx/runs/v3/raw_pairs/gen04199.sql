with recent_stores as (
    select *
    from stores
    where city in ('Berlin', 'London') or opened_date >= '2024-11-15'
),
summary as (
    select city, min(sqft) as min_sqft, max(sqft) as max_sqft
    from recent_stores
    group by city
)
select city, min_sqft, max_sqft
from summary
where min_sqft > 10600
order by 1
