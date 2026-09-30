with with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by txn_type order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by txn_type order by txn_time, id) + interval 30 days then 1 else 0 end as new_group_flag
    from transactions
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
)
select
    id,
    txn_type,
    amount,
    txn_time,
    burst_id
from sessionized
