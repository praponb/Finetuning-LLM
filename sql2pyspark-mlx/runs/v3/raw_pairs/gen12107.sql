with src as (
    select *
    from subscriptions
    where status in ('EXPIRED', 'PAUSED', 'ACTIVE') and year(end_date) >= 2025
),
SubscriptionsStats as (
    select plan, count(monthly_fee) as monthly_fee_count, avg(monthly_fee) as avg_monthly_fee
    from src
    group by plan
)
select plan, monthly_fee_count, avg_monthly_fee
from SubscriptionsStats
order by plan
limit 5;
