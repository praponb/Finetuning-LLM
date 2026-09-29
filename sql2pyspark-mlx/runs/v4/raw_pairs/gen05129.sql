with with_windows as (
    -- Running and moving aggregates
    select
        *,
        count(amount) over (partition by order_id) as order_id_count_amount,
        sum(amount) over (order by paid_at, id rows between unbounded preceding and current row) as cumulative_amount,
        lead(amount, 1) over (partition by order_id order by paid_at, id) as next_amount,
        count(amount) over (partition by method order by cast(paid_at as bigint) range between 604800 preceding and current row) as amount_count_7d
    from payments
),
ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by order_id, method order by next_amount desc nulls last, id) as latest_rank
    from with_windows
    qualify latest_rank = 1
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        method,
        amount,
        status,
        amount_count_7d,
        date_trunc('year', paid_at) as paid_at_period,
        round(latest_rank / cumulative_amount, 2) as latest_rank_per_cumulative_amount,
        coalesce(method, 'N/A') as method_filled
    from ranked
    where paid_at > timestamp '2024-03-27 15:00:00'
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by status order by latest_rank_per_cumulative_amount nulls last, id) as dedup_rank
    from base
    qualify dedup_rank = 1
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at_period) over (partition by method order by paid_at_period, id) is null
                 or paid_at_period > lag(paid_at_period) over (partition by method order by paid_at_period, id) + interval 30 days then 1 else 0 end as new_group_flag
    from deduped
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by method order by paid_at_period, id rows between unbounded preceding and current row) as island_id
    from flagged
),
flagged_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at_period) over (partition by status order by paid_at_period, id) is null
                 or paid_at_period > lag(paid_at_period) over (partition by status order by paid_at_period, id) + interval 60 days then 1 else 0 end as is_new_burst
    from sessionized
),
sessionized_payments as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by status order by paid_at_period, id rows between unbounded preceding and current row) as session_no
    from flagged_2
),
stats as (
    -- Aggregate with conditional metrics
    select
        status,
        date_trunc('year', paid_at_period) as year_start,
        count_if(status = 'FAILED') as failed_events,
        count(*) as row_count,
        sum(new_group_flag) filter (where method = 'bank_transfer') as bank_transfer_total_new_group_flag,
        grouping(status) as gid
    from sessionized_payments
    group by rollup (status, date_trunc('year', paid_at_period))
)
select row_count, bank_transfer_total_new_group_flag
from stats
order by bank_transfer_total_new_group_flag desc nulls last, row_count desc;
