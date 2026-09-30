with base as (
    select
        *,
        datediff(current_date(), end_date) as days_since_end_date,
        round(monthly_fee, 2) as monthly_fee_rounded
    from subscriptions
    where end_date >= date '2023-10-07'
),
with_windows as (
    select
        id,
        start_date,
        sum(days_since_end_date) over (partition by year(end_date), plan order by start_date, id rows between unbounded preceding and current row) as cumulative_days_since_end_date,
        first_value(monthly_fee, true) over (partition by customer_id order by start_date, id rows between unbounded preceding and current row) as first_known_monthly_fee
    from base
),
base_subscriptions as (
    select
        *,
        coalesce(cumulative_days_since_end_date, 0) as cumulative_days_since_end_date_filled,
        dayofweek(start_date) as start_date_dayofweek
    from with_windows
    where start_date > date '2023-08-16'
),
with_windows_subscriptions as (
    select
        *,
        lag(cumulative_days_since_end_date) over (order by cumulative_days_since_end_date, id) as prev_cumulative_days_since_end_date,
        lag(start_date_dayofweek) over (order by cumulative_days_since_end_date, id) as prev_start_date_dayofweek,
        row_number() over (order by cumulative_days_since_end_date, id) as seq_no
    from base_subscriptions
),
scored as (
    select
        id,
        start_date,
        cumulative_days_since_end_date_filled,
        prev_start_date_dayofweek,
        sum(cumulative_days_since_end_date_filled) over (order by first_known_monthly_fee, id rows between 3 preceding and current row) as last4_sum_cumulative_days_since_end_date_filled,
        min(cumulative_days_since_end_date_filled) over (order by first_known_monthly_fee, id rows between 3 preceding and current row) as last4_min_cumulative_days_since_end_date_filled,
        max(cumulative_days_since_end_date_filled) over (order by first_known_monthly_fee, id rows between 3 preceding and current row) as last4_max_cumulative_days_since_end_date_filled,
        row_number() over (order by first_known_monthly_fee desc, id) as event_seq
    from with_windows_subscriptions
    where first_known_monthly_fee < 220
),
filtered as (
    select *, last_day(start_date) as start_date_month_end
    from scored
    where last4_min_cumulative_days_since_end_date_filled <= 50
)
select
    id,
    start_date,
    last4_sum_cumulative_days_since_end_date_filled,
    last4_min_cumulative_days_since_end_date_filled,
    last4_max_cumulative_days_since_end_date_filled,
    start_date_month_end
from filtered
order by last4_max_cumulative_days_since_end_date_filled
