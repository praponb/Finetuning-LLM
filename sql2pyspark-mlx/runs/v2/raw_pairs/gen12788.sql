select
    a.id,
    a.opened_date,
    a.account_type,
    t.total_amount,
    t.latest_txn_time
from accounts a
inner join (select account_id, sum(amount) as total_amount, max(txn_time) as latest_txn_time
    from transactions
    group by account_id) t on a.id = t.account_id
