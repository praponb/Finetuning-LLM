with enriched as (
    select
        id,
        start_date,
        monthly_fee,
        min(monthly_fee) over (partition by plan, status order by start_date, id rows between unbounded preceding and current row) as running_min_monthly_fee,
        count(monthly_fee) over (partition by plan, status order by start_date, id rows between unbounded preceding and current row) as running_count_monthly_fee,
        avg(monthly_fee) over (partition by plan, status order by start_date, id rows between unbounded preceding and current row) as running_avg_monthly_fee,
        round(monthly_fee / sum(monthly_fee) over (partition by month(end_date)), 4) as monthly_fee_share
    from subscriptions
),
metrics as (
    select
        *,
        sum(running_count_monthly_fee) over (order by running_avg_monthly_fee, id rows between unbounded preceding and current row) as running_running_count_monthly_fee,
        first_value(running_count_monthly_fee) ignore nulls over (order by start_date, id rows between unbounded preceding and current row) as first_known_running_count_monthly_fee
    from enriched
),
filtered as (
    select
        *,
        case when running_avg_monthly_fee >= 212 then 'High' when running_avg_monthly_fee >= 81 then 'Medium' else 'Low' end as running_avg_monthly_fee_band,
        least(running_running_count_monthly_fee, running_min_monthly_fee) as least_running_running_count_monthly_fee_running_min_monthly_fee
    from metrics
    where start_date > to_date('2023-03-26')
),
metrics_subscriptions as (
    select
        id,
        running_min_monthly_fee,
        running_avg_monthly_fee,
        monthly_fee_share,
        running_running_count_monthly_fee,
        first_known_running_count_monthly_fee,
        least_running_running_count_monthly_fee_running_min_monthly_fee,
        lead(least_running_running_count_monthly_fee_running_min_monthly_fee, 1) over w_part as next_least_running_running_count_monthly_fee_running_min_monthly_fee,
        row_number() over w_part as event_seq
    from filtered
    window w_part as (order by running_count_monthly_fee, id)
),
metrics_subscriptions_2 as (
    select
        id,
        monthly_fee_share,
        first_known_running_count_monthly_fee,
        least_running_running_count_monthly_fee_running_min_monthly_fee,
        avg(running_min_monthly_fee) over w_hist as last7_avg_running_min_monthly_fee,
        min(running_min_monthly_fee) over w_hist as last7_min_running_min_monthly_fee,
        sum(running_min_monthly_fee) over w_hist as last7_sum_running_min_monthly_fee,
        max(running_min_monthly_fee) over w_hist as last7_max_running_min_monthly_fee
    from metrics_subscriptions
    window w_hist as (order by monthly_fee_share, id rows between 6 preceding and current row)
)
select
    id,
    monthly_fee_share,
    first_known_running_count_monthly_fee,
    least_running_running_count_monthly_fee_running_min_monthly_fee,
    last7_min_running_min_monthly_fee
from metrics_subscriptions_2
