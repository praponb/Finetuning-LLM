with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, year(txn_time) txn_time_year
    from transactions
    where not is_flagged
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by txn_type order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by txn_type order by txn_time, id) + interval 30 days then 1 else 0 end as session_start_flag
    from filtered
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as session_no
    from with_gaps
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        txn_time_year,
        session_start_flag,
        session_no,
        ntile(3) over (partition by account_id order by amount, id) amount_bucket,
        ntile(4) over (order by txn_time_year desc, id) txn_time_year_bucket
    from sessionized
)
-- Window metrics over several different partitions
select
    *,
    sum(amount_bucket) over (order by cast(txn_time as bigint) range between 2592000 preceding and current row) amount_bucket_sum_30d,
    last_value(txn_time_year) ignore nulls over (partition by txn_type order by txn_time_year, id rows between unbounded preceding and current row) last_known_txn_time_year,
    sum(amount) over (partition by txn_type order by txn_time_year_bucket, id rows between unbounded preceding and current row) running_amount,
    sum(txn_time_year_bucket) over (partition by txn_type) txn_type_sum_txn_time_year_bucket
from enriched
