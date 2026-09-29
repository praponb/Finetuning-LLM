with with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over w_recent as amount_dense_rank,
        round(amount / sum(amount) over (partition by txn_type, merchant), 4) as amount_share,
        stddev_samp(amount) over w_grp as account_id_stddev_samp_amount
    from transactions
    window w_recent as (partition by month(txn_time), account_id order by amount desc nulls last), w_grp as (partition by account_id)
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        account_id_stddev_samp_amount + amount_share as account_id_stddev_samp_amount_plus_amount_share,
        date_trunc('week', txn_time) as txn_time_period
    from with_windows
    where txn_time > timestamp '2023-07-25 05:00:00'
),
enriched as (
    -- Attach accounts attributes
    select s.*, p.account_type
    from base s
    left join accounts p
        on s.account_id = p.id
    where p.customer_id in (5, 26)
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by txn_type, merchant order by account_id_stddev_samp_amount_plus_amount_share desc nulls last, id) as row_num
    from enriched
    qualify row_num = 1
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by txn_type order by txn_time, id) > interval '90' day then 1 else 0 end as new_group_flag
    from ranked
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as island_id
    from with_gaps
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by txn_type order by txn_time, id) > interval '30' day then 1 else 0 end as new_group_flag_2
    from sessionized
),
sessionized_transactions as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag_2) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as session_no
    from gaps
),
with_windows_transactions as (
    -- Window metrics over several different partitions
    select
        *,
        sum(new_group_flag_2) over (partition by account_type order by session_no, id rows between unbounded preceding and current row) as cumulative_new_group_flag_2,
        avg(account_id_stddev_samp_amount) over (partition by account_id) as account_id_avg_account_id_stddev_samp_amount
    from sessionized_transactions
)
-- Join a dimension and aggregate
select
    p.account_type as account_account_type,
    a.txn_type,
    count(*) as cnt,
    avg(a.account_id_avg_account_id_stddev_samp_amount) as avg_account_id_avg_account_id_stddev_samp_amount,
    max(a.amount_dense_rank) as max_amount_dense_rank,
    grouping_id() as grouping_level
from with_windows_transactions a
left join accounts p on a.account_id = p.id
group by rollup (p.account_type, a.txn_type)
order by account_account_type nulls last
