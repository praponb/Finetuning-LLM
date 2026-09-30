with with_windows as (
    -- Ranking and gap features
    select
        *,
        min(monthly_fee) over (partition by customer_id, plan order by start_date, id rows between 6 preceding and current row) as last7_min_monthly_fee,
        max(monthly_fee) over (partition by customer_id, plan order by start_date, id rows between 6 preceding and current row) as last7_max_monthly_fee,
        count(monthly_fee) over (partition by customer_id, plan order by start_date, id rows between 6 preceding and current row) as last7_count_monthly_fee,
        lag(monthly_fee, 1) over (partition by date_trunc('month', end_date), status order by start_date, id) as prev_monthly_fee
    from subscriptions
    where month(end_date) in (4, 6, 12)
),
scored as (
    -- Ranking and gap features
    select
        id,
        start_date,
        last7_min_monthly_fee,
        last7_max_monthly_fee,
        last7_count_monthly_fee,
        sum(prev_monthly_fee) over w_running as running_prev_monthly_fee,
        last_value(prev_monthly_fee, true) over (partition by status order by last7_min_monthly_fee, id rows between unbounded preceding and current row) as last_known_prev_monthly_fee,
        sum(last7_max_monthly_fee) over (partition by date_trunc('week', start_date) order by monthly_fee, id rows between unbounded preceding and current row) as running_last7_max_monthly_fee
    from with_windows
    window w_running as (order by last7_min_monthly_fee, id rows between unbounded preceding and current row)
),
scored_subscriptions as (
    -- Window metrics over several different partitions
    select
        *,
        rank() over (order by running_prev_monthly_fee) as running_prev_monthly_fee_rank,
        lead(last7_count_monthly_fee, 1) over (order by last_known_prev_monthly_fee, id) as next_last7_count_monthly_fee,
        avg(last7_max_monthly_fee) over (order by start_date, id rows between 6 preceding and current row) as last7_max_monthly_fee_avg_6
    from scored
),
metrics as (
    -- Ranking and gap features
    select
        *,
        avg(next_last7_count_monthly_fee) over w_part as last3_avg_next_last7_count_monthly_fee,
        sum(next_last7_count_monthly_fee) over w_part as last3_sum_next_last7_count_monthly_fee,
        min(next_last7_count_monthly_fee) over w_part as last3_min_next_last7_count_monthly_fee,
        sum(last7_max_monthly_fee_avg_6) over w_hist as cumulative_last7_max_monthly_fee_avg_6
    from scored_subscriptions
    window w_part as (order by last7_count_monthly_fee, id rows between 2 preceding and current row), w_hist as (order by running_prev_monthly_fee, id rows between unbounded preceding and current row)
),
base as (
    -- Keep recent, valid rows
    select
        *,
        last7_count_monthly_fee - cumulative_last7_max_monthly_fee_avg_6 as last7_count_monthly_fee_minus_cumulative_last7_max_monthly_fee_avg_6,
        round(running_last7_max_monthly_fee, 0) as running_last7_max_monthly_fee_rounded
    from metrics
    where start_date >= to_date('2023-03-28')
),
metrics_subscriptions as (
    -- Ranking and gap features
    select
        id,
        start_date,
        last7_min_monthly_fee,
        last7_count_monthly_fee,
        running_prev_monthly_fee_rank,
        next_last7_count_monthly_fee,
        last3_avg_next_last7_count_monthly_fee,
        last3_min_next_last7_count_monthly_fee,
        cumulative_last7_max_monthly_fee_avg_6,
        running_last7_max_monthly_fee_rounded,
        rank() over (order by next_last7_count_monthly_fee nulls first) as next_last7_count_monthly_fee_rank,
        lead(running_prev_monthly_fee, 2) over (order by running_last7_max_monthly_fee, id) as next_running_prev_monthly_fee,
        stddev_samp(running_last7_max_monthly_fee_rounded) over (partition by date_trunc('week', start_date)) as start_date_period_stddev_samp_running_last7_max_monthly_fee_rounded,
        round(last7_max_monthly_fee_avg_6 / sum(last7_max_monthly_fee_avg_6) over (partition by date_trunc('week', start_date)), 4) as last7_max_monthly_fee_avg_6_share
    from base
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        start_date,
        last7_count_monthly_fee,
        running_prev_monthly_fee_rank,
        next_last7_count_monthly_fee,
        last3_avg_next_last7_count_monthly_fee,
        last3_min_next_last7_count_monthly_fee,
        cumulative_last7_max_monthly_fee_avg_6,
        next_last7_count_monthly_fee_rank,
        next_running_prev_monthly_fee,
        last7_max_monthly_fee_avg_6_share,
        cast(last7_max_monthly_fee_avg_6_share as int) as last7_max_monthly_fee_avg_6_share_int
    from metrics_subscriptions
    where next_last7_count_monthly_fee_rank <= 5
)
-- Ranking and gap features
select
    *,
    max(last7_max_monthly_fee_avg_6_share_int) over (order by next_running_prev_monthly_fee, id rows between unbounded preceding and current row) as running_max_last7_max_monthly_fee_avg_6_share_int,
    count(last7_max_monthly_fee_avg_6_share_int) over (order by next_running_prev_monthly_fee, id rows between unbounded preceding and current row) as running_count_last7_max_monthly_fee_avg_6_share_int,
    avg(last7_max_monthly_fee_avg_6_share_int) over (order by next_running_prev_monthly_fee, id rows between unbounded preceding and current row) as running_avg_last7_max_monthly_fee_avg_6_share_int,
    ntile(5) over (order by last3_avg_next_last7_count_monthly_fee desc, id) as last3_avg_next_last7_count_monthly_fee_bucket
from filtered
