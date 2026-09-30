select
    a.id,
    a.account_type,
    a.customer_id,
    t.last_txn_time
from accounts a
left join (select account_id, max(txn_time) as last_txn_time
    from transactions
    where account_id in (5, 11, 17, 22)
    group by account_id) t on a.id = t.account_id
order by account_type;
