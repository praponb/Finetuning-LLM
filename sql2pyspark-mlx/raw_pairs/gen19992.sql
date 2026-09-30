with recent_stores as (
    select *
    from stores
    where city in ('New York', 'Berlin', 'Singapore')
),
stores_stats as (
    select month(opened_date) as opened_month, max(sqft) - min(sqft) as sqft_range
    from recent_stores
    group by month(opened_date)
)
select opened_month, sqft_range
from stores_stats
where sqft_range < 56
order by opened_month desc
limit 10;
