with flagged as (
    select *, case when lag(txn_time) over (partition by txn_type order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by txn_type order by txn_time, id) + interval '60' day then 1 else 0 end as session_start_flag
    from transactions
),
sessions_transactions as (
    select *, sum(session_start_flag) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as session_id
    from flagged
)
select
    s.id,
    s.account_id,
    s.txn_type,
    s.txn_time,
    s.merchant,
    s.is_flagged,
    s.session_start_flag,
    s.session_id,
    (select max(t2.amount) from transactions t2 where t2.account_id = s.account_id) as max_transaction_amount
from sessions_transactions s
order by id desc;
