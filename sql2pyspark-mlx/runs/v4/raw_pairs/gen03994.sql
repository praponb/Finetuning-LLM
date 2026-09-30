with with_prev as (
    -- Previous event time per key
    select *, lag(txn_time) over (partition by txn_type order by txn_time, id) as prev_txn_time
    from transactions
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_txn_time is null or txn_time > prev_txn_time + interval '60' day then 1 else 0 end as new_group_flag
    from with_prev
)
-- Running sum of the flag = session number
select *, sum(new_group_flag) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as island_id
from gaps
