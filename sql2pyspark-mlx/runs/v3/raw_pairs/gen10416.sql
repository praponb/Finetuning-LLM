with account_stats as (
    select account_id, count(distinct amount) as unique_txn_types, sort_array(collect_set(txn_type)) as txn_type_list
    from transactions
    group by account_id
)
select
    a.id,
    a.status,
    a.balance,
    as.unique_txn_types,
    as.txn_type_list
from accounts a
left join account_stats as on a.id = as.account_id
order by a.id
