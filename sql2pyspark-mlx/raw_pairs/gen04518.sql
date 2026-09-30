with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when is_flagged then 'Yes' else 'No' end as is_flagged_label,
        coalesce(merchant, 'DIRECT') as merchant_clean
    from transactions
    where merchant is not null
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by txn_type order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by txn_type order by txn_time, id) + interval '90' day then 1 else 0 end as session_start_flag
    from filtered
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as island_id
    from with_gaps
),
deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        merchant_clean
    from (
        select *, row_number() over (partition by merchant order by session_start_flag desc, id) as row_num
        from islands
    ) x
    where row_num = 1
),
cleaned as (
    -- Keep recent, valid rows
    select *, date_trunc('MONTH', txn_time) as txn_time_period
    from deduped
    where merchant is distinct from 'Starbucks'
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        amount,
        txn_time,
        merchant_clean,
        txn_time_period,
        round(amount / sum(amount) over (partition by to_date(txn_time)), 4) as amount_share,
        avg(amount) over (partition by merchant order by txn_time_period, id rows between 5 preceding and current row) as amount_avg_5,
        row_number() over (partition by txn_type, merchant order by txn_time_period desc, id) as seq_no
    from cleaned
    where txn_time >= '2023-08-25'
)
-- Running and moving aggregates
select
    id,
    amount,
    txn_time_period,
    seq_no,
    min(amount_share) over (order by amount_avg_5, id rows between 2 preceding and current row) as last3_min_amount_share,
    max(amount_share) over (order by amount_avg_5, id rows between 2 preceding and current row) as last3_max_amount_share,
    avg(amount_share) over (order by amount_avg_5, id rows between 2 preceding and current row) as last3_avg_amount_share,
    sum(amount_avg_5) over (order by cast(txn_time as bigint) range between 7776000 preceding and current row) as amount_avg_5_sum_90d
from metrics;
