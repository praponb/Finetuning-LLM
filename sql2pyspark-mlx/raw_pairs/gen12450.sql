with with_windows as (
    -- Ranking and gap features
    select
        id,
        start_date,
        count(monthly_fee) over (partition by customer_id order by monthly_fee, id rows between unbounded preceding and current row) as running_count_monthly_fee,
        min(monthly_fee) over (partition by customer_id order by monthly_fee, id rows between unbounded preceding and current row) as running_min_monthly_fee,
        avg(monthly_fee) over (partition by customer_id order by monthly_fee, id rows between unbounded preceding and current row) as running_avg_monthly_fee
    from subscriptions
    where customer_id is not null
)
select
    id,
    start_date,
    running_count_monthly_fee,
    running_avg_monthly_fee
from with_windows
order by running_avg_monthly_fee;
