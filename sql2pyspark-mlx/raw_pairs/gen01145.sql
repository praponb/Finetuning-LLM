with Src as (
    select *, case when plan = 'enterprise' then 1 else 0 end as is_enterprise
    from subscriptions
    where start_date < '2024-10-01'
),
summary as (
    select plan, max(end_date) as latest_end_date
    from Src
    group by plan
)
select *
from summary
order by plan
limit 5;
