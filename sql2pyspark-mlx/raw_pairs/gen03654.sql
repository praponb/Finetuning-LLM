with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        start_date,
        monthly_fee,
        datediff(end_date, start_date) as days_start_date_to_end_date
    from subscriptions
    where customer_id is not null
      and status = 'CANCELLED'
),
valid as (
    -- Basic cleansing and defaults
    select *, last_day(start_date) as start_date_month_end
    from filtered
    where start_date >= date '2023-09-03'
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        start_date,
        monthly_fee,
        days_start_date_to_end_date,
        start_date_month_end,
        sum(days_start_date_to_end_date) over (order by monthly_fee, id rows between unbounded preceding and current row) as cumulative_days_start_date_to_end_date,
        cume_dist() over (order by monthly_fee) as monthly_fee_cume_dist,
        ntile(5) over (order by days_start_date_to_end_date, id) as days_start_date_to_end_date_bucket
    from valid
),
base as (
    -- Basic cleansing and defaults
    select *, greatest(monthly_fee, cumulative_days_start_date_to_end_date) as greatest_monthly_fee_cumulative_days_start_date_to_end_date
    from enriched
    where year(start_date_month_end) = 2025
),
filtered_subscriptions as (
    -- Filter the raw rows and derive helper columns
    select *, monthly_fee_cume_dist * greatest_monthly_fee_cumulative_days_start_date_to_end_date as monthly_fee_cume_dist_times_greatest_monthly_fee_cumulative_days_start_date_to_end_date
    from base
    where start_date >= to_date('2023-07-05')
),
metrics as (
    -- Ranking and gap features
    select
        *,
        lead(monthly_fee_cume_dist) over w_hist as next_monthly_fee_cume_dist,
        lag(monthly_fee_cume_dist) over w_hist as prev_monthly_fee_cume_dist,
        lag(greatest_monthly_fee_cumulative_days_start_date_to_end_date) over w_hist as prev_greatest_monthly_fee_cumulative_days_start_date_to_end_date,
        lead(greatest_monthly_fee_cumulative_days_start_date_to_end_date) over w_hist as next_greatest_monthly_fee_cumulative_days_start_date_to_end_date,
        row_number() over w_hist as event_seq,
        dense_rank() over (order by days_start_date_to_end_date desc nulls last) as days_start_date_to_end_date_dense_rank
    from filtered_subscriptions
    where start_date_month_end < '2024-01-01'
    window w_hist as (order by monthly_fee_cume_dist_times_greatest_monthly_fee_cumulative_days_start_date_to_end_date, id)
),
base_subscriptions as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        event_seq / nullif(next_greatest_monthly_fee_cumulative_days_start_date_to_end_date, 0) as event_seq_per_next_greatest_monthly_fee_cumulative_days_start_date_to_end_date,
        monthly_fee_cume_dist / nullif(cumulative_days_start_date_to_end_date, 0) as monthly_fee_cume_dist_per_cumulative_days_start_date_to_end_date
    from metrics
    where start_date >= date '2023-05-06'
)
-- Grouped metrics
select
    case when event_seq_per_next_greatest_monthly_fee_cumulative_days_start_date_to_end_date >= 66 then 'HIGH' else 'LOW' end as event_seq_per_next_greatest_monthly_fee_cumulative_days_start_date_to_end_date_tier,
    stddev_samp(monthly_fee) as monthly_fee_stddev,
    percentile_approx(prev_greatest_monthly_fee_cumulative_days_start_date_to_end_date, 0.5) as prev_greatest_monthly_fee_cumulative_days_start_date_to_end_date_p50,
    min(greatest_monthly_fee_cumulative_days_start_date_to_end_date) as min_greatest_monthly_fee_cumulative_days_start_date_to_end_date
from base_subscriptions
group by event_seq_per_next_greatest_monthly_fee_cumulative_days_start_date_to_end_date_tier
order by monthly_fee_stddev desc;
