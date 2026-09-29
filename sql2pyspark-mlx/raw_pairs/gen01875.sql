with gaps as (
    select *, case when lag(txn_time) over (partition by merchant order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by merchant order by txn_time, id) + interval 7 days then 1 else 0 end as is_new_burst
    from transactions
)
select
    id,
    account_id,
    txn_time,
    merchant,
    is_flagged,
    sum(is_new_burst) over (partition by merchant order by txn_time, id rows between unbounded preceding and current row) as session_id
from gaps
