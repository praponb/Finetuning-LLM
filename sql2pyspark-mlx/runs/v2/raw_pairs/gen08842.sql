with valid_subscriptions as (
    select *
    from subscriptions
    where start_date >= '2024-11-01' or status in ('CANCELLED', 'EXPIRED')
),
SubscriptionsStats as (
    select
        plan,
        date_trunc('MONTH', end_date) as end_month,
        max(monthly_fee) as max_monthly_fee,
        min(monthly_fee) as min_monthly_fee
    from valid_subscriptions
    group by plan, date_trunc('MONTH', end_date)
)
select plan, max_monthly_fee, min_monthly_fee
from SubscriptionsStats
where min_monthly_fee < 220
order by plan, min_monthly_fee desc
