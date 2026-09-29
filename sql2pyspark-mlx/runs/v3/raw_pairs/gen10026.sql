with TransactionsByAccount as (
    select account_id, min(txn_time) as first_txn_time, count(*) as cnt
    from transactions
    group by account_id
)
select
    a.balance,
    a.customer_id,
    T.first_txn_time,
    T.cnt
from accounts a
left join TransactionsByAccount T on a.id = T.account_id
where T.cnt > 8
order by a.balance desc;
