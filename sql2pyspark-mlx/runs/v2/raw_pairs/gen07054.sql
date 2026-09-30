with account_stats as (
    select account_id, concat_ws(', ', sort_array(collect_set(txn_type))) as txn_type_values
    from transactions
    where amount > 2500
    group by account_id
)
select
    a.id,
    a.status,
    a.account_type,
    as.txn_type_values
from accounts a
inner join account_stats as on a.id = as.account_id
order by a.account_type
