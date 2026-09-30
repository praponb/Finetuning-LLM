with FilteredStores as (
    select *
    from stores
    where year(opened_date) = 2025
),
agg as (
    select
        quarter(opened_date) as opened_quarter,
        city,
        round((100 * sum(case when city = 'Berlin' then 1 else 0 end)) / count(*), 1) as pct_berlin,
        count(case when city = 'Bangkok' then 1 end) as bangkok_count
    from FilteredStores
    group by quarter(opened_date), city
)
select *
from agg
order by bangkok_count desc, opened_quarter, city
limit 20;
