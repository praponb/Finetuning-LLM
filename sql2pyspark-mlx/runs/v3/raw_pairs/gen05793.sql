with src as (
    select *
    from stores
    where city = 'New York' and opened_date > '2024-11-01'
),
agg as (
    select region, round((100 * sum(case when city = 'London' then 1 else 0 end)) / count(*), 1) as pct_london, sum(case when region = 'East' then 1 else 0 end) as east_count
    from src
    group by region
)
select region, pct_london, east_count
from agg
where east_count > 67
order by region desc;
