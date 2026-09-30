with src as (
    select *, round(monthly_fee, 1) monthly_fee_rounded
    from subscriptions
    where customer_id in (9, 28, 29, 32)
),
subscriptions_stats as (
    select customer_id, avg(monthly_fee) avg_monthly_fee, min(monthly_fee_rounded) min_monthly_fee_rounded
    from src
    group by customer_id
)
select *
from subscriptions_stats
order by customer_id desc;
