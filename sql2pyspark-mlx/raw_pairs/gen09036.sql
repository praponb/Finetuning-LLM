with filtered as (
    -- Keep recent, valid rows
    select
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        date_trunc('week', start_date) as start_date_period,
        round(monthly_fee * 1.07, 2) as monthly_fee_adjusted
    from subscriptions
    where end_date > date '2023-10-26'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        max(monthly_fee) over w_grp as last3_max_monthly_fee,
        avg(monthly_fee) over w_grp as last3_avg_monthly_fee,
        min(monthly_fee) over w_grp as last3_min_monthly_fee
    from filtered
    window w_grp as (partition by status order by start_date, id rows between 2 preceding and current row)
),
filtered_subscriptions as (
    -- Basic cleansing and defaults
    select *, date_format(start_date_period, 'MMM yyyy') as start_date_period_label
    from with_windows
    where monthly_fee < 160
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when start_date_period - lag(start_date_period) over (partition by status order by start_date_period, id) > interval '7' day then 1 else 0 end as is_new_session
    from filtered_subscriptions
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by status order by start_date_period, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
)
-- Aggregate with conditional metrics
select
    case when monthly_fee_adjusted >= 72 then 'LARGE' else 'SMALL' end as monthly_fee_adjusted_tier,
    round(sum(last3_max_monthly_fee) / nullif(count(case when plan = 'enterprise' then 1 end), 0), 2) as last3_max_monthly_fee_per_enterprise,
    round(avg(last3_avg_monthly_fee), 2) as avg_last3_avg_monthly_fee,
    avg(last3_min_monthly_fee) filter (where status = 'CANCELLED') as cancelled_avg_last3_min_monthly_fee,
    max(start_date_period) as last_seen,
    round(avg(last3_avg_monthly_fee), 2) as avg_last3_avg_monthly_fee_2,
    count(*) filter (where plan = 'standard') as standard_rows
from islands
group by rollup (case when monthly_fee_adjusted >= 72 then 'LARGE' else 'SMALL' end)
order by avg_last3_avg_monthly_fee desc nulls last
