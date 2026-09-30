with scored as (
    -- Ranking and gap features
    select
        id,
        plan,
        end_date,
        monthly_fee,
        status,
        min(monthly_fee) over (partition by plan order by monthly_fee, id rows between unbounded preceding and current row) as running_min_monthly_fee,
        avg(monthly_fee) over (partition by plan order by monthly_fee, id rows between unbounded preceding and current row) as running_avg_monthly_fee,
        count(monthly_fee) over (partition by plan order by monthly_fee, id rows between unbounded preceding and current row) as running_count_monthly_fee
    from subscriptions
),
scored_subscriptions as (
    -- Running and moving aggregates
    select
        id,
        plan,
        end_date,
        status,
        count(running_avg_monthly_fee) over w_part as last3_count_running_avg_monthly_fee,
        max(running_avg_monthly_fee) over w_part as last3_max_running_avg_monthly_fee,
        min(running_avg_monthly_fee) over w_part as last3_min_running_avg_monthly_fee
    from scored
    window w_part as (partition by status, plan order by monthly_fee, id rows between 2 preceding and current row)
),
scored_subscriptions_2 as (
    -- Running and moving aggregates
    select
        *,
        avg(last3_count_running_avg_monthly_fee) over w_recent as running_avg_last3_count_running_avg_monthly_fee,
        max(last3_count_running_avg_monthly_fee) over w_recent as running_max_last3_count_running_avg_monthly_fee,
        count(last3_count_running_avg_monthly_fee) over w_recent as running_count_last3_count_running_avg_monthly_fee,
        min(last3_count_running_avg_monthly_fee) over w_recent as running_min_last3_count_running_avg_monthly_fee
    from scored_subscriptions
    where status = 'CANCELLED'
    window w_recent as (partition by plan order by last3_max_running_avg_monthly_fee, id rows between unbounded preceding and current row)
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        plan,
        end_date,
        status,
        last3_max_running_avg_monthly_fee,
        last3_min_running_avg_monthly_fee,
        running_avg_last3_count_running_avg_monthly_fee,
        running_max_last3_count_running_avg_monthly_fee,
        running_count_last3_count_running_avg_monthly_fee,
        running_min_last3_count_running_avg_monthly_fee,
        least(running_avg_last3_count_running_avg_monthly_fee, running_min_last3_count_running_avg_monthly_fee) as least_running_avg_last3_count_running_avg_monthly_fee_running_min_last3_count_running_avg_monthly_fee
    from scored_subscriptions_2
    where end_date >= date '2023-04-14'
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        plan,
        end_date,
        status,
        running_count_last3_count_running_avg_monthly_fee,
        running_min_last3_count_running_avg_monthly_fee,
        least_running_avg_last3_count_running_avg_monthly_fee_running_min_last3_count_running_avg_monthly_fee,
        round(running_max_last3_count_running_avg_monthly_fee / running_avg_last3_count_running_avg_monthly_fee, 2) as running_max_last3_count_running_avg_monthly_fee_per_running_avg_last3_count_running_avg_monthly_fee
    from cleaned
    where end_date >= date '2023-05-12'
)
select
    id,
    plan,
    end_date,
    status,
    running_count_last3_count_running_avg_monthly_fee,
    running_min_last3_count_running_avg_monthly_fee,
    least_running_avg_last3_count_running_avg_monthly_fee_running_min_last3_count_running_avg_monthly_fee,
    running_max_last3_count_running_avg_monthly_fee_per_running_avg_last3_count_running_avg_monthly_fee
from base
order by least_running_avg_last3_count_running_avg_monthly_fee_running_min_last3_count_running_avg_monthly_fee desc nulls first, end_date;
