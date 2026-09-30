with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by account_id order by txn_time, id) > interval '14' day then 1 else 0 end as new_group_flag
    from transactions
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as session_no
    from gaps
)
-- Multi-level aggregation
select
    account_id,
    min(session_no) as min_session_no,
    size(collect_set(merchant)) as num_merchants
from sessionized
group by account_id
having min(session_no) >= 0
