with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        round(amount, 2) as amount_rounded,
        month(txn_time) as txn_time_month
    from transactions
    where merchant is not null
      and merchant is distinct from 'Lazada'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        amount,
        amount_rounded,
        avg(amount) over (partition by merchant order by amount, id rows between unbounded preceding and current row) as running_avg_amount,
        max(amount) over (partition by merchant order by amount, id rows between unbounded preceding and current row) as running_max_amount,
        count(amount) over (partition by merchant order by amount, id rows between unbounded preceding and current row) as running_count_amount
    from filtered
    where merchant <> 'Shell'
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        amount,
        amount_rounded,
        running_avg_amount,
        running_max_amount,
        running_count_amount,
        ceil(running_count_amount) as running_count_amount_whole
    from with_windows
    where running_count_amount <= 8
),
scored as (
    -- Ranking and gap features
    select
        *,
        min(running_max_amount) over (order by amount, id rows between 3 preceding and current row) as last4_min_running_max_amount,
        max(running_max_amount) over (order by amount, id rows between 3 preceding and current row) as last4_max_running_max_amount,
        count(running_max_amount) over (order by amount, id rows between 3 preceding and current row) as last4_count_running_max_amount
    from valid
),
filtered_transactions as (
    -- Filter the raw rows and derive helper columns
    select *, case when amount_rounded >= 66 then 'High' when amount_rounded >= 30 then 'Medium' else 'Low' end as amount_rounded_tier
    from scored
    where amount between 1400 and 3100
),
daily as (
    -- Aggregate with conditional metrics
    select
        case when running_max_amount >= 3800 then 'PREMIUM' else 'STANDARD' end as running_max_amount_tier,
        case when last4_count_running_max_amount >= 15 then 'LARGE' else 'SMALL' end as last4_count_running_max_amount_tier,
        approx_percentile(running_count_amount, 0.95) as running_count_amount_p95,
        count(distinct id) as distinct_ids
    from filtered_transactions
    group by grouping sets ((case when running_max_amount >= 3800 then 'PREMIUM' else 'STANDARD' end, case when last4_count_running_max_amount >= 15 then 'LARGE' else 'SMALL' end), (case when running_max_amount >= 3800 then 'PREMIUM' else 'STANDARD' end), ())
)
select
    last4_count_running_max_amount_tier,
    running_count_amount_p95,
    distinct_ids
from daily
order by distinct_ids desc nulls last;
