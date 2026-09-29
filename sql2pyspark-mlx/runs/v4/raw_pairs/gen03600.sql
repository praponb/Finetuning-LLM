with account_summary as (
    select account_id, count(distinct amount) as unique_txn_types, max(txn_time) as last_txn_time
    from transactions
    where amount < 2000
    group by account_id
)
select
    a.id,
    a.account_type,
    as.unique_txn_types,
    as.last_txn_time
from accounts a
left outer join account_summary as on a.id = as.account_id
where as.unique_txn_types >= 5;
