with flagged as (
    select *, case when txn_time - lag(txn_time) over (partition by account_id order by txn_time, id) > interval '14' day then 1 else 0 end as is_new_burst
    from transactions
),
islands as (
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        sum(is_new_burst) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as session_no
    from flagged
)
select
    id,
    amount,
    txn_time,
    merchant,
    session_no
from islands
order by session_no desc, amount;
