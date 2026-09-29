with account_stats as (
    select account_id, min(txn_time) as first_txn_time
    from transactions
    group by account_id
)
select a.balance, as.first_txn_time
from accounts a
inner join account_stats as on a.id = as.account_id
order by as.first_txn_time desc;
