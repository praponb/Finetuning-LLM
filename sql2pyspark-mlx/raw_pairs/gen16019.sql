with scored as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(monthly_fee, true) over (partition by status order by start_date, id rows between unbounded preceding and current row) first_known_monthly_fee,
        lag(monthly_fee, 1, 0) over (partition by customer_id order by monthly_fee, id) prev_monthly_fee,
        last_value(monthly_fee, true) over w_grp last_known_monthly_fee,
        avg(monthly_fee) over (order by start_date, id rows between 6 preceding and current row) monthly_fee_avg_6,
        lag(monthly_fee, 1) over (order by start_date, id) prev_monthly_fee_2
    from subscriptions
    where monthly_fee < 160
    window w_grp as (partition by customer_id order by monthly_fee, id rows between unbounded preceding and current row)
)
select
    id,
    start_date,
    end_date,
    prev_monthly_fee,
    last_known_monthly_fee,
    monthly_fee_avg_6,
    prev_monthly_fee_2
from scored
order by start_date nulls first;
