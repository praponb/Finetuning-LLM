with filtered as (
    -- Keep recent, valid rows
    select
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        concat(txn_type, ', ', merchant) as txn_type_merchant
    from transactions
    where txn_time > timestamp '2023-09-26 10:00:00'
      and merchant is not null
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        avg(amount) over (partition by txn_type order by txn_time asc, id asc rows between 5 preceding and 1 preceding) as amount_avg_5_prior,
        count(amount) over (partition by merchant) as merchant_count_amount,
        max(amount) over w_hist as txn_type_max_amount,
        ntile(5) over (partition by merchant order by amount desc, id asc) as amount_bucket,
        row_number() over w_ordered as event_seq
    from filtered
    window w_hist as (partition by txn_type), w_ordered as (order by txn_time asc, id asc)
),
latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by txn_type, merchant order by merchant_count_amount desc nulls last, id asc) as rank_in_group
    from with_windows
    qualify rank_in_group = 1
),
metrics as (
    -- Ranking and gap features
    select
        *,
        lead(amount_avg_5_prior, 1) over w_hist as next_amount_avg_5_prior,
        avg(txn_type_max_amount) over (partition by merchant, txn_type order by cast(txn_time as bigint) asc range between 7776000 preceding and current row) as txn_type_max_amount_avg_90d,
        row_number() over (partition by merchant, txn_type order by merchant_count_amount desc, id asc) as seq_no,
        last_value(amount) ignore nulls over w_part as last_known_amount,
        last_value(amount_bucket, true) over (partition by txn_type order by txn_time asc, id asc rows between unbounded preceding and current row) as last_known_amount_bucket
    from latest
    window w_hist as (partition by merchant, txn_type order by amount asc, id asc), w_part as (partition by merchant, txn_type order by rank_in_group asc, id asc rows between unbounded preceding and current row)
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by txn_type order by txn_time asc, id asc) is null
                 or txn_time > lag(txn_time) over (partition by txn_type order by txn_time asc, id asc) + interval 90 days then 1 else 0 end as is_new_burst
    from metrics
),
sessions_transactions as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by txn_type order by txn_time asc, id asc rows between unbounded preceding and current row) as island_id
    from flagged
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by txn_type order by txn_time asc, id asc) > interval '30' day then 1 else 0 end as session_start_flag
    from sessions_transactions
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by txn_type order by txn_time asc, id asc rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from sessionized
    qualify rank() over (partition by merchant order by amount asc nulls first) = 1
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        txn_type,
        last_known_amount,
        amount
    from ranked
) src
unpivot (
    metric_value for metric in (last_known_amount, amount)
)
