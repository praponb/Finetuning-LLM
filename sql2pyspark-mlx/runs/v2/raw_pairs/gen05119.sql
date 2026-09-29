with filtered_subscriptions as (
    select *
    from subscriptions
    where status = 'ACTIVE' and end_date >= '2023-03-17'
),
SubscriptionsStats as (
    select status, avg(monthly_fee) as avg_monthly_fee
    from filtered_subscriptions
    group by status
)
select status, avg_monthly_fee
from SubscriptionsStats
order by status desc
limit 50;
