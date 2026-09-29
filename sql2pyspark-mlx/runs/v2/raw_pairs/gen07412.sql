select
    a.id,
    a.status,
    a.account_type,
    t.txn_type_values
from accounts a
inner join (select account_id, concat_ws(', ', sort_array(collect_set(txn_type))) txn_type_values
    from transactions
    where account_id in (12, 17, 29, 35)
    group by account_id) t on a.id = t.account_id;
