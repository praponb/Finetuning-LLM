with with_prev as (
    -- Previous event time per key
    select *, lag(txn_time) over (partition by txn_type order by txn_time asc, id asc) as prev_txn_time
    from transactions
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_txn_time is null or txn_time > prev_txn_time + interval '90' day then 1 else 0 end as is_new_burst
    from with_prev
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by txn_type order by txn_time asc, id asc rows between unbounded preceding and current row) as session_id
    from with_gaps
)
select
    id,
    account_id,
    txn_type,
    amount,
    merchant,
    is_flagged,
    prev_txn_time,
    is_new_burst,
    case when txn_type rlike '^(REFUND|DEBIT)$' then 'GROUP_B' else 'EXTENDED' end as txn_type_zone
from sessionized
order by merchant asc;
