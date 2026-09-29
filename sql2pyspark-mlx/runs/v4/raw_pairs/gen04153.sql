with valid_subscriptions as (
    select *
    from subscriptions
    where status in ('PAUSED', 'EXPIRED')
),
summary as (
    select status, round(avg(monthly_fee), 2) avg_monthly_fee, max(monthly_fee) max_monthly_fee
    from valid_subscriptions
    group by status
)
select *
from summary
order by avg_monthly_fee desc, status
limit 10
