with recent_subscriptions as (
    select *, weekofyear(end_date) as end_date_weekofyear
    from subscriptions
    where status = 'CANCELLED'
),
agg as (
    select status, min(end_date_weekofyear) as min_end_date_weekofyear
    from recent_subscriptions
    group by status
)
select status, min_end_date_weekofyear
from agg
order by min_end_date_weekofyear desc;
