with recent_subscriptions as (
    select *, weekofyear(end_date) as end_date_weekofyear
    from subscriptions
    where status = 'CANCELLED'
),
summary as (
    select plan, count(monthly_fee) as monthly_fee_count
    from recent_subscriptions
    group by plan
)
select *
from summary;
