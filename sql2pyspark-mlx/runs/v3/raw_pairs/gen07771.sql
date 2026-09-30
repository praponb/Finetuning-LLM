with base as (
    select *
    from subscriptions
    where not (customer_id in (2, 17) and status != 'CANCELLED' and customer_id = 40)
),
SubscriptionsStats as (
    select status, max(end_date) last_end_date, min(monthly_fee) min_monthly_fee
    from base
    group by status
)
select status, last_end_date, min_monthly_fee
from SubscriptionsStats
order by status asc, last_end_date desc;
