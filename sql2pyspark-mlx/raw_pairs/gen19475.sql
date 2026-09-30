with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by txn_type order by txn_time, id) > interval 30 days then 1 else 0 end as is_new_session
    from transactions
),
sessions_transactions as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as session_id
    from flagged
)
select
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.merchant,
    s.is_new_session,
    s.session_id,
    case when txn_type rlike '^(REFUND|DEBIT)$' then 'OTHER_ZONE' when txn_type rlike '^(CREDIT|FEE)$' then 'GROUP_B' else 'GROUP_A' end as txn_type_zone,
    (select sum(t2.amount) from transactions t2 where t2.account_id = s.account_id) as sum_transaction_amount
from sessions_transactions s
order by session_id desc;
